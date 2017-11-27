package generator;

import org.kohsuke.args4j.Option;

/**
 * CommandLineParams is the class implementation of commandline parameters for the kafka-data-gen application.
 *
 * @author      Don Tregonning (dtregonning)
 * @version     1.0
 * @since       1.0
 */
public class CommandLineParams {
    @Option(name="-message-count", usage="(Required)Sets amount of messages to create and send to kafka topic")
    public String messageCount;

    @Option(name="-message-size", usage="(Required)Sets size of messages to create")
    public String messageSize;

    @Option(name="-eps", usage="(Required)Amount of events per second to send to Kafka")
    public String eps;

    @Option(name="-worker-thread-count", usage="Event generating worker threads, default = 4")
    public String workerThreadCount;

    @Option(name="-topic", usage="(Required)Kafka Topic to send messages to")
    public String topic;

    @Option(name="-bootstrap.servers", usage="(Required) Kafka Servers to send messages to")
    public String bootStrapServers;

    @Option(name="-acks", usage="Acknowledgement Scheme (all, 1, 0),  default = all")
    public String acks;

    @Option(name="-kafka-retries", usage="Kafka retries amount,  default = 0")
    public String retries;

    @Option(name="-kafka-batch-size", usage="Kafka batch size amount,  default = 1000")
    public String kafkaBatchSize;

    @Option(name="-kafka-linger", usage="Kafka linger setting(ms) ,  default = 1ms")
    public String kafkaLingerms;

    @Option(name="-kafka-buffer-memory", usage="Kafka buffer amount,  default = 16384")
    public String kafkaBufferMemory;

    public String toString() {
        return "[Command Line Parameters]"
        + "{ message-count: " + messageCount
        + ", message-size: " + messageSize
        + ", topic : " + topic
        + ", eps: " + eps
        + ", worker-thread-count: " + workerThreadCount
        + ", bootstrap.servers: " + bootStrapServers
        + ", acks: " + acks
        + ", kafka-retries: " + retries
        + ", kafka-batch-size: " + kafkaBatchSize
        + ", kafka-linger: " + kafkaLingerms
        + ", kafka-buffer-memory: " + kafkaBufferMemory + "}";
    }
}
