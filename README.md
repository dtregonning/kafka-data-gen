# kafka-data-gen
kafka-data-gen is a simple message generation tool that will create defined size events and send them to
kafka at a specific interval or at a certain rate(Events per Second).

#### Build
kafka-data-gen is a gradle project and can be built using the `gradle install` command.
A jar file is created which can be used for data generation. kafka-data-gen.jar

#### Examples
java -jar build/libs/kafka-data-gen.jar -message-count 1000 -message-size 256 -topic kafka-demo 
-bootstrap.servers "localhost:9092" -acks all -kafka-retries 0 -kafka-batch-size 16384 -kafka-linger 1 -kafka-buffer-memory 33554432

#### Command Line Options

* `message-count`: Sets amount of messages to create.
* `message-size`: Sets size of messages to create.
* `eps`: Amount of events per second to send to Kafka.
* `topic`: Kafka Topic to send messages to.
* `bootstrap.servers`: Kafka Servers to send messages to.
* `acks`: Acknowledgement Scheme (all, 1, 0).
* `kafka-retries `: Kafka retries amount.
* `kafka-batch-size`: Kafka batch size amount.
* `kafka-linger`: Kafka linger setting(ms).
* `kafka-buffer-memory`: Kafka buffer amount.