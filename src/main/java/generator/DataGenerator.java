package generator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;

public class DataGenerator {
    private static Logger logger = LogManager.getLogger(DataGenerator.class);

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
        }

        Properties props = new Properties();
        props = parseKafkaArguments(params, props);

        long startTime = System.currentTimeMillis();

        int workers = 0;
        if(params.workerThreadCount != null) {
            workers = Integer.parseInt(params.workerThreadCount);
        }else {
            workers = 4;
        }
        logger.info("Worker count configured to: " + workers);

        EPSToken epsToken = new EPSToken();
        EPSThread thread_01 = new EPSThread("RefreshTokenThread", epsToken, props, params);
        EPSThread[] epsThreadArray = new EPSThread[workers];

        for(int i = 0; i < epsThreadArray.length; i++) {
            String threadName = "WorkerThread-" + i;
            epsThreadArray[i] = new EPSThread(threadName, epsToken, props, params);
        }
        try {
            thread_01.thrd.join();
            for(int i = 0; i < epsThreadArray.length; i++) {
                epsThreadArray[i].thrd.join();
            }
        } catch (InterruptedException exc) {
            logger.error("Thread Interrupted");
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info(epsToken.getMessageKey() + " messages created and sent in " + (elapsedTime / 1000) + " seconds");
        logger.info("Events Per Second of " + (long)(epsToken.getMessageKey())/(elapsedTime / 1000));
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