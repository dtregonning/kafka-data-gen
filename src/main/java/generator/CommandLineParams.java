package generator;

import java.io.File;
import org.kohsuke.args4j.Option;

/**
 * Created by dtregonning on 9/13/17.
 */
public class CommandLineParams {
    @Option(name="-message-count", usage="Sets amount of messages to create")
    public String messageCount;

    @Option(name="-message-size", usage="Sets size of messages to create")
    public String messageSize;

    @Option(name="-message-delay", usage="Sets delay between each message(ms)")
    public String messageDelay;

    @Option(name="-output-file", usage="Name of output file to create or append to")
    public String outputFile;

    @Option(name="-topic", usage="Kafka Topic tp send messages to")
    public String topic;

    @Option(name="-bootstrap.servers", usage="Kafka Servers to send messages to")
    public String bootStrapServers;

    @Option(name="-acks", usage="Acknowledgement Scheme (all, 1, 0)")
    public String acks;

    @Option(name="-kafka-retries", usage="Kafka retries amount")
    public String retries;

    @Option(name="-kafka-batch-size", usage="Kafka batch size amount")
    public String kafkaBatchSize;

    @Option(name="-kafka-linger", usage="Kafka linger setting(ms)")
    public String kafkaLingerms;

    @Option(name="-kafka-buffer-memory", usage="Kafka buffer amount")
    public String kafkaBufferMemory;

    public void run() {
        System.out.println("Command Line Paramaters");
        System.out.println("- message-count: " + messageCount);
        System.out.println("- message-size: " + messageSize);
        System.out.println("- message-delay: " + messageDelay);
        System.out.println("- file: " + outputFile);
        System.out.println("- topic: " + topic);
    }
}
