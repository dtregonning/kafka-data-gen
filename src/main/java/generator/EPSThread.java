package generator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class EPSThread implements Runnable {
    private static Logger logger = LogManager.getLogger(EPSThread.class);
    private static EPSToken epsTokenObj;
    private static Properties props;
    private static CommandLineParams params;
    private static MetricsCalculator metricsCalc;
    Thread thrd;
    private static int counter = 0;

    EPSThread(String name, EPSToken eps, Properties props, CommandLineParams params) {
        thrd = new Thread(this, name);
        epsTokenObj = eps;
        this.params = params;
        this.props = props;
        thrd.start();
        metricsCalc = new MetricsCalculator(Integer.parseInt(params.workerThreadCount));
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
                        logger.debug("Tokens increased by " + eps);
                        Thread.sleep(1000);
                    }
                    epsTokenObj.increaseTokens(leftOvers);
                    epsTokenObj.toggleFinished();
                }
                else {
                    epsTokenObj.increaseTokens(Integer.parseInt(params.messageCount));
                    do{
                        Thread.sleep(500);
                    } while(epsTokenObj.getMessageKey() < Integer.parseInt(params.messageCount));
                    epsTokenObj.toggleFinished();
                    logger.info("Total Message count reached, cleaning up program for exit.");
                }

            } else if(thrd.getName().compareTo("MetricsCalculatorThread") == 0 && Boolean.parseBoolean(params.outputToKafka) == true) {
                if(Boolean.parseBoolean(params.outputToStdout) != true) {
                    do {
                        Thread.sleep(5000);
                        logger.info("Current Record Send Rate is: " + metricsCalc.getKafkaProducerMetrics("record-send-rate", "producer-metrics"));
                    } while (epsTokenObj.complete() == false);
                }
            }
            else {
                if(Boolean.parseBoolean(params.outputToKafka) == true) {
                    if (Boolean.parseBoolean(params.outputToStdout) == true) {
                        do {
                            if (epsTokenObj.takeToken()) {
                                shipEvent(epsTokenObj, params);
                            }
                        } while (epsTokenObj.complete() == false);
                    } else {
                        Producer<String, String> producer = new KafkaProducer<>(props);
                        if (!metricsCalc.addProducer(producer)) {
                            logger.warn("Error adding producer for metrics Calculator, Metric Calculations may be incorrect" + thrd.getName());
                        }

                        do {
                            if (epsTokenObj.takeToken()) {
                                shipEvent(producer, epsTokenObj, params);
                            }
                        } while (epsTokenObj.complete() == false);

                        producer.close();
                    }
                }
                else if(Boolean.parseBoolean(params.outputToEventhubs) == true){
                    final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                            .setNamespaceName(props.getProperty("eventhub.namespace"))
                            .setEventHubName(props.getProperty("eventhub.name"))
                            .setSasKeyName(props.getProperty("eventhub.saskeyname"))
                            .setSasKey(props.getProperty("eventhub.saskey"));
                    logger.info(connStr);


                    final Gson gson = new GsonBuilder().create();

                    // The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
                    // This enables the user to segregate their thread pool based on the work load.
                    // This pool can then be shared across multiple EventHubClient instances.
                    // The following sample uses a single thread executor, as there is only one EventHubClient instance,
                    // handling different flavors of ingestion to Event Hubs here.
                    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

                    // Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
                    // It is always a best practice to reuse these instances. The following sample shows this.
                    final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

                    do {
                        if (epsTokenObj.takeToken()) {
                            shipEventhubEvent(ehClient, epsTokenObj, params);
                        }
                    } while (epsTokenObj.complete() == false);

                    ehClient.closeSync();
                    executorService.shutdown();
                }
            }
        } catch (InterruptedException | EventHubException | IOException exc ) {
            System.out.println("Thread Interrupted");
        }
    }

    public static void shipEvent(Producer<String, String> producer, EPSToken epsTokenObj, CommandLineParams params) {
        int sequenceNumber = epsTokenObj.getMessageKeyAndInc();

        //TODO: Smarter Live Logging, hardcoded 10000 value. 10% of total messages?
        if(sequenceNumber % 100000 == 0) { logger.info("Current message with sequence number: " + sequenceNumber); }

        byte[] event = createEvent(params, sequenceNumber);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(params.topic, Integer.toString(sequenceNumber), new String(event));

            if(Boolean.parseBoolean(params.includeKafkaHeaders) == true)
                includeKafkaHeaders(record, sequenceNumber);
            producer.send(record);
            logger.debug("Event batched" + record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void shipEventhubEvent(EventHubClient ehClient, EPSToken epsTokenObj , CommandLineParams params) {
        try {
            int sequenceNumber = epsTokenObj.getMessageKeyAndInc();
            if(sequenceNumber % 100000 == 0) { logger.info("Current message with sequence number: " + sequenceNumber); }
            byte[] event = createEvent(params, sequenceNumber);

            //byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
            EventData sendEvent = EventData.create(event);

        // Send - not tied to any partition
        // Event Hubs service will round-robin the events across all Event Hubs partitions.
        // This is the recommended & most reliable way to send to Event Hubs.
            ehClient.sendSync(sendEvent);

            logger.debug("Event batched" + event.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void shipEvent(EPSToken epsTokenObj , CommandLineParams params) {
        int sequenceNumber = epsTokenObj.getMessageKeyAndInc();

        if(sequenceNumber % 100000 == 0) { logger.info("Current message with sequence number: " + sequenceNumber); }

        byte[] event = createEvent(params, sequenceNumber);
        try {
            String s = new String(event);
            System.out.println(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ProducerRecord<String, String> includeKafkaHeaders(ProducerRecord<String, String> record, int sequenceNumber) {
        Random random = new Random();
        //same header values
        if(Integer.parseInt(params.headerGenProfile) == 0) {
            record.headers()
                    .add("header_index", "header_index".getBytes())
                    .add("header_host", "header_host".getBytes())
                    .add("header_source", "header_source".getBytes())
                    .add("header_sourcetype", "splunk:kafka:headers".getBytes());
        }

        // completely random headers
        else if(Integer.parseInt(params.headerGenProfile) == 1) {
            String random_host = "splunk_host" + random.nextInt();
            String random_source = "splunk_source" + random.nextInt();
            String random_sourcetype = "splunk_sourcetype" + random.nextInt();

            record.headers()
                    .add("header_index", "header_index".getBytes())
                    .add("header_host", random_host.getBytes())
                    .add("header_source", random_source.getBytes())
                    .add("header_sourcetype", random_sourcetype.getBytes());
        }

        // 1 header alternating randomly
        else if(Integer.parseInt(params.headerGenProfile) == 2) {
            String random_source = "";
            if(sequenceNumber % 100 == 0) {
                random_source = "splunk_source" + random.nextInt();
            }

            record.headers()
                    .add("header_index", "header_index".getBytes())
                    .add("header_host", "header_host".getBytes())
                    .add("header_source", random_source.getBytes())
                    .add("header_sourcetype", "splunk:kafka:headers".getBytes());
        }

        //with extra headers
        else if(Integer.parseInt(params.headerGenProfile) == 3) {

            record.headers()
                    .add("header_index", "header_index".getBytes())
                    .add("header_host", "header_host_profile_3".getBytes())
                    .add("header_source", "header_source_profile_3".getBytes())
                    .add("header_sourcetype", "splunk:kafka:headers:profile_3".getBytes())
                    .add("random_header_1", "random_header_1".getBytes())
                    .add("random_header_2", "random_header_2".getBytes());
        }

        else if(Integer.parseInt(params.headerGenProfile) == 4) {

            record.headers()
                    .add("header_index", "header_index".getBytes())
                    .add("header_host", "header_host_profile_3".getBytes())
                    .add("header_source", "header_source_profile_3".getBytes())
                    .add("header_sourcetype", "splunk:kafka:headers:profile_3".getBytes())
                    .add("java -jar build/libs/kafka-data-gen.jar -message-count 100000 -message-size 256 -topic kafka-demo-header-7 -bootstrap.servers \"localhost:9092\" -acks all -kafka-retries 0 -kafka-batch-size 60000 -kafka-linger 1 -kafka-buffer-memory 33554432 -eps 500 -generate-kafka-headers true -header-gen-profile 4" +
                     "", "random_header_1".getBytes())
                    .add("random_header_2", "random_header_2".getBytes());
        }

        else if(Integer.parseInt(params.headerGenProfile) == 5) {
            if(sequenceNumber % 50 == 0) {
                counter++;
            }

            record.headers()
                    .add("header_index", ("header_index").getBytes())
                    .add("header_host", ("header_host_profile" + counter).getBytes())
                    .add("header_source", ("header_source_profile" + counter).getBytes())
                    .add("header_sourcetype", ("splunk:kafka:headers:profile" + counter).getBytes());
        }
        else if(Integer.parseInt(params.headerGenProfile) == 6) {
            int counter = 1;
            if(sequenceNumber % 2 == 0) {
                counter = 2;
            }
            if(sequenceNumber % 3 == 0) {
                counter = 3;
            }

            if(sequenceNumber % 4 == 0) {
                counter = 4;
            }

            record.headers()
                    .add("header_index", ("header_index").getBytes())
                    .add("header_host", ("header_host_profile" + counter).getBytes())
                    .add("header_source", ("header_source_profile" + counter).getBytes())
                    .add("header_sourcetype", ("splunk:kafka:headers:profile" + counter).getBytes());
        }

        else if(Integer.parseInt(params.headerGenProfile) == -1) {
            return record;
        }
        else {
            logger.info("Incorrect ");
        }
        return record;
    }



    public static byte[] createEvent(CommandLineParams params, int eventKey) {
        String s = "";
        if((params.eventFormat.equalsIgnoreCase("kinesis"))) {
            KinesisFirehoseMessage message = new KinesisFirehoseMessage();
            s = message.toString();
        }
        else {
            DataGenMessage message = new DataGenMessage(Integer.parseInt(params.messageSize), eventKey);
            s = message.toJSON();
        }
        byte event[] = s.getBytes();
        return event;
    }
}