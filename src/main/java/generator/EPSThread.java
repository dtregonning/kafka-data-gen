package generator;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

class EPSThread implements Runnable {
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
        int eps = Integer.parseInt(params.eps);
        try {
            if (thrd.getName().compareTo("RefreshTokenThread") == 0) {
                if(eps != 0) {
                    int totalRuns = Integer.parseInt(params.messageCount) / eps;
                    int leftOvers = Integer.parseInt(params.messageCount) % eps;
                    for (int i = 0; i < totalRuns; i++) {
                        epsTokenObj.increaseTokens(eps);
                        Thread.sleep(1000);
                    }
                    epsTokenObj.increaseTokens(leftOvers);
                    epsTokenObj.toggleFinished();
                } else {
                    do {
                        Thread.sleep(1000);
                    } while (epsTokenObj.getMessageKey() < Integer.parseInt(params.messageCount));
                    epsTokenObj.toggleFinished();
                }

            } else {
                Producer<String, String> producer = new KafkaProducer<>(props);
                do {
                    if (eps == 0) {
                        shipEvent(producer, epsTokenObj, params);
                    } else {
                        if (epsTokenObj.takeToken()) { shipEvent(producer, epsTokenObj, params); }
                    }
                } while (epsTokenObj.complete() == false);
                producer.close();
            }
        } catch (InterruptedException exc) {
            System.out.println("Thread Interrupted");
        }
    }

    public static void shipEvent(Producer<String, String> producer,EPSToken epsTokenObj , CommandLineParams params) {
        byte[] event = createEvent(params);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(params.topic, Integer.toString(epsTokenObj.getKey()), new String(event));
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] createEvent(CommandLineParams params) {
        DataGenMessage message = new DataGenMessage(Integer.parseInt(params.messageSize));
        String s = message.toJSON();
        byte event[] = s.getBytes();
        return event;
    }
}