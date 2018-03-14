package generator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;

/**
 * DataGenerator is the main method and entry point for the kafka-data-gen application, The DataGenerator will:
 * <ul>
 * <li> Read and Parse in command line values</li>
 * <li> Set any default values for parameters</li>
 * <li> Create all threads needed and loop till finished</li>
 * <li> Output metrics on program complete.</li>
 * </ul>
 *
 * @author      Don Tregonning (dtregonning)
 * @version     1.0
 * @since       1.0
 */
public class DataGenerator {
    private static final Logger logger = LogManager.getLogger(DataGenerator.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Data Generator");

        CommandLineParams params = new CommandLineParams();
        CmdLineParser parser = new CmdLineParser(params);

        logger.info("Parsing CLI arguments");
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            logger.error(e.getMessage());
            parser.printUsage(System.err);
            System.exit(2);
        }

        if (params.outputToStdout == "false") {
            // Check for required parameters
            if (params.bootStrapServers == null || params.topic == null) {
                logger.error("Missing required commandline parameter - quiting kafka-data-gen - Exit Status 1");
                System.exit(1);
            }

        }
        //Set defaults for non required params. And log printout of value default or configured
        if(params.workerThreadCount == null) { params.workerThreadCount = "4";}
        if(params.eps == null) { params.eps = "0";}
        if(params.eventFormat == null) { params.eventFormat = "json";}
        if(params.messageSize == null) { params.messageSize = "256";}
        if(params.messageCount == null) { params.messageSize = "10000";}
        logger.info(params);

        //Create and configure Kafka Producer variables. Store in Properties object
        Properties props = new Properties();
        props = parseKafkaArguments(params, props);

        EPSToken epsToken = new EPSToken();

        /* Thread Implementation.
        * RefreshTokenThread will be used to throttle and control message creation. If a token is not available at a
        * given time a message cannot be created and sent. By using tokens we can control throughput. This thread will
        * refresh tokens. Logic implemented in EPSToken().
        * MetricsCalculatorThread is responsible for periodically reporting metrics to the log file.
        * WorkThreads - create, batch and sent events to a Kafka topic.
        * */
        EPSThread thread_01 = new EPSThread("RefreshTokenThread", epsToken, props, params);
        EPSThread thread_02 = new EPSThread("MetricsCalculatorThread", epsToken, props, params);
        EPSThread[] epsThreadArray = new EPSThread[Integer.parseInt(params.workerThreadCount)];

        long startTime = System.currentTimeMillis();

        for(int i = 0; i < epsThreadArray.length; i++) {
            String threadName = "WorkerThread-" + i;
            epsThreadArray[i] = new EPSThread(threadName, epsToken, props, params);
        }
        try {
            thread_01.thrd.join();
            thread_02.thrd.join();
            for(int i = 0; i < epsThreadArray.length; i++) {
                epsThreadArray[i].thrd.join();
            }
        } catch (InterruptedException exc) {
            logger.error("Thread Interrupted");
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        // Program Exit statistics
        logger.info(epsToken.getMessageKey() + " messages created and sent in " + (elapsedTime / 1000) + " seconds");
        logger.info("Events Per Second of " + (long)(epsToken.getMessageKey())/(elapsedTime / 1000));
        logger.info("Program run and complete without interruption");
    }

    public static Properties parseKafkaArguments(CommandLineParams params, Properties props) {
        try {
            props.put("bootstrap.servers", params.bootStrapServers);
            if(params.acks != null) { props.put("acks", params.acks);}
            else { props.put("acks", "all"); }
            if(params.retries != null) { props.put("retries", params.retries); }
            else {props.put("retries", "0"); }
            if(params.kafkaBatchSize != null) { props.put("batch.size", params.kafkaBatchSize); }
            else { props.put("batch.size", "1000");}
            if(params.kafkaLingerms != null) { props.put("linger.ms", params.kafkaLingerms); }
            else { props.put("linger.ms", "1"); }
            if(params.kafkaBufferMemory != null) { props.put("buffer.memory", params.kafkaBufferMemory); }
            else {props.put("buffer.memory", "16384"); }
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } catch (java.lang.NullPointerException e) {
            logger.error(e.getMessage());
            logger.error("Config Value Not provided");
        }
        return props;
    }
}