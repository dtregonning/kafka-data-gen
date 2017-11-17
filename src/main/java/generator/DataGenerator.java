package generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;

import org.apache.kafka.clients.producer.*;

/**
 * Created by dtregonning on 9/12/17.
 */
public class DataGenerator {
    private static Logger logger = LoggerFactory.getLogger(DataGenerator.class);

    public static void main(String[] args) throws IOException {
        logger.info("Starting Kafka Data Generator");

        CommandLineParams params = new CommandLineParams();
        CmdLineParser parser = new CmdLineParser(params);

        logger.info("Parsing CLI arguments");
        parseCLIArguments(parser, args);

        Properties props = new Properties();
        Producer<String, String> producer = new KafkaProducer<>(parseKafkaArguments(params, props));

        long event_delay = 0; //500 ms - 2 events a second.
        long secondVar = 1;

        if (params.eps != null && Integer.parseInt(params.eps) > 1) {
            logger.info("EPS value found - Calculating required delay");
            event_delay = (long)((secondVar/Double.parseDouble(params.eps)* 1000000)) ;
            logger.info("Delay is " + event_delay + " microSeconds");
        }

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < Integer.parseInt(params.messageCount); i++) {
            byte[] event = createEvent(params);
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(params.topic, Integer.toString(i), new String(event));
                producer.send(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
            delayNextEvent(event_delay);
        }
        producer.close();

        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info(params.messageCount + " messages created and sent in " + (elapsedTime / 1000) + " seconds");
        logger.info("EPS of " + (Long.parseLong(params.messageCount)/(elapsedTime / 1000)));
    }

    public static void parseCLIArguments(CmdLineParser parser, String[] args) {
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            logger.error(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    public static Properties parseKafkaArguments(CommandLineParams params, Properties props) {
        try {
            props.put("bootstrap.servers", params.bootStrapServers);
            if(params.acks != null) { props.put("acks", params.acks);}
            else { props.put("acks", "all"); }
            if(params.retries != null) { props.put("retries", params.retries); }
            else {props.put("retries", "0"); }
            if(params.kafkaBatchSize != null) { props.put("batch.size", params.kafkaBatchSize); }
            else { props.put("batch.size", "1000"); }
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

    public static byte[] createEvent(CommandLineParams params) {
        DataGenMessage message = new DataGenMessage(Integer.parseInt(params.messageSize));
        String s = message.toJSON();
        byte event[] = s.getBytes();
        logger.debug("Event created - " + s);
        return event;
    }

    public static ProducerRecord createProducerRecord(CommandLineParams params, byte[] event, int eventID) {
        ProducerRecord kafkaRecord = new ProducerRecord(params.topic, eventID, event);
        return kafkaRecord;
    }

    public static void delayNextEvent(long eventDelay) {
        try {
            TimeUnit.MICROSECONDS.sleep(eventDelay);
        } catch (InterruptedException x) {
            System.err.println(x);
        }
    }
}