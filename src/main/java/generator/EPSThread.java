package generator;

import com.sun.javafx.collections.MappingChange;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Metric;

import java.util.Properties;
import java.util.Map;

class EPSThread implements Runnable {
    private static Logger logger = LogManager.getLogger(EPSThread.class);
    Thread thrd;
    EPSToken epsTokenObj;
    Properties props;
    CommandLineParams params;

    EPSThread(String name, EPSToken eps, Properties props, CommandLineParams params) {
        thrd = new Thread(this, name);
        epsTokenObj = eps;
        this.params = params;
        this.props = props;
        thrd.start();
    }

    public void run() {
        logger.debug("Thread started - " + thrd.getName());
        int eps = Integer.parseInt(params.eps);
        try {
            if (thrd.getName().compareTo("RefreshTokenThread") == 0) {
                if(eps != 0) {
                    logger.debug("EPS value not set to 0, throttled throughput. EPS is:  " + eps);
                    int totalRuns = Integer.parseInt(params.messageCount) / eps;
                    int leftOvers = Integer.parseInt(params.messageCount) % eps;
                    for (int i = 0; i < totalRuns; i++) {
                        epsTokenObj.increaseTokens(eps);
                        logger.debug("Tokens increased by "+  eps);
                        Thread.sleep(1000);
                    }
                    epsTokenObj.increaseTokens(leftOvers);
                    epsTokenObj.toggleFinished();

                } else {
                    epsTokenObj.increaseTokens(Integer.parseInt(params.messageCount));
                    do{
                        Thread.sleep(500);
                    } while(epsTokenObj.getMessageKey() < Integer.parseInt(params.messageCount));
                    epsTokenObj.toggleFinished();
                    logger.info("Total Message count reached, cleaning up.");
                }

            } else {
                logger.debug("EPS value is 0, Maximum throughput");

                Producer<String, String> producer = new KafkaProducer<>(props);
                do {
                    if (epsTokenObj.takeToken()) { shipEvent(producer, epsTokenObj, params); }
                } while (epsTokenObj.complete() == false);

                Map<MetricName,? extends Metric> metrics = producer.metrics();
                for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                    String name = entry.getKey().name();
                    String group = entry.getKey().group();
                    if(name.equalsIgnoreCase("record-send-rate")  && group.equalsIgnoreCase("producer-metrics")) {
                        System.out.println(name + " " + group + ": " + entry.getValue().value());
                    }
                }
                producer.close();
            }
        } catch (InterruptedException exc) {
            System.out.println("Thread Interrupted");
        }
    }

    public static void shipEvent(Producer<String, String> producer,EPSToken epsTokenObj , CommandLineParams params) {
        int sequenceNumber = epsTokenObj.getKey();
        byte[] event = createEvent(params, sequenceNumber);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(params.topic, Integer.toString(sequenceNumber), new String(event));
            producer.send(record);
            logger.debug("Event sent" + record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] createEvent(CommandLineParams params, int eventKey) {
        DataGenMessage message = new DataGenMessage(Integer.parseInt(params.messageSize), eventKey);
        String s = message.toJSON();
        byte event[] = s.getBytes();
        return event;
    }
}