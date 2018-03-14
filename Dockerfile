FROM java:latest
WORKDIR /
ADD ./build/libs/kafka-data-gen.jar kafka-data-gen.jar
CMD java -jar kafka-data-gen.jar  -message-count 20000000 -message-size $MESSAGE -topic kafka-demo -bootstrap.servers "localhost:9092" -acks all -kafka-retries 0 -kafka-batch-size 60000 -kafka-linger 1 -kafka-buffer-memory 33554432 -eps 0 -output-stdout true
