package generator;

import static java.nio.file.StandardOpenOption.*;
import java.nio.file.*;
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
    public static void main(String[] args) throws IOException {

        CommandLineParams params = new CommandLineParams();
        CmdLineParser parser = new CmdLineParser(params);

        parseCLIArguments(parser, args);

        Properties props = new Properties();
        Producer<String, String> producer = new KafkaProducer<>(parseKafkaArguments(params, props));

        //Functionality for writing to File
        //Path p = Paths.get(params.outputFile);
        //OutputStream outstream = new BufferedOutputStream(Files.newOutputStream(p, CREATE, APPEND));

        for(int i = 0; i < Integer.parseInt(params.messageCount); i++) {
            byte[] event = createEvent(params);
            //printEventToFile(params, event, i, outstream);
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(params.topic, Integer.toString(i), new String(event));
                producer.send(record);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            delayNextEvent(params);
        }
        producer.close();
    }

    public static void parseCLIArguments(CmdLineParser parser, String[] args) {
        try {
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }

    public static Properties parseKafkaArguments(CommandLineParams params, Properties props) {
        props.put("bootstrap.servers", params.bootStrapServers);
        props.put("acks", params.acks);
        props.put("retries", params.retries);
        props.put("batch.size", params.kafkaBatchSize);
        props.put("linger.ms", params.kafkaLingerms);
        props.put("buffer.memory", params.kafkaBufferMemory);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    public static byte[] createEvent(CommandLineParams params) {
        DataGenMessage message = new DataGenMessage(Integer.parseInt(params.messageSize));
        String s = message.toJSON();
        byte event[] = s.getBytes();
        return event;
    }

    public static ProducerRecord createProducerRecord(CommandLineParams params, byte[] event, int eventID) {
        ProducerRecord kafkaRecord = new ProducerRecord(params.topic, eventID, event);
        return kafkaRecord;
    }

    public static void printEventToFile(CommandLineParams params, byte[] event, int eventID, OutputStream outstream) throws IOException {
        outstream.write(event, 0, event.length);
        System.out.println("Event Created:"  + eventID);
        outstream.flush();
    }

    public static void delayNextEvent(CommandLineParams params) {
        try {
            TimeUnit.MILLISECONDS.sleep(Long.parseLong(params.messageDelay));
        } catch (InterruptedException x) {
            System.err.println(x);
        }
    }
}