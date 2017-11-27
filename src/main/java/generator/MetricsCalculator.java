package generator;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Created by dtregonning on 11/21/17.
 */
public class MetricsCalculator {
    Producer<String, String>[] producerArray;
    private static Logger logger = LogManager.getLogger(EPSThread.class);

    int arrayIndex = 0;

    public MetricsCalculator(int workerThreadCount) {
        producerArray = (Producer<String, String>[])new Producer[workerThreadCount];
    }

    public synchronized void addProducer(Producer<String, String> producer) {
        producerArray[arrayIndex] = producer;
        logger.info("Im here");
        arrayIndex++;
    }

    public int getArrayIndex() {
        return arrayIndex;
    }

    public void getMetrics() {
        double recordSendRate = 0;
        for(int i = 0; i < arrayIndex; i++){
            Producer<String, String> producer = producerArray[i];
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                String name = entry.getKey().name();
                String group = entry.getKey().group();
                if (name.equalsIgnoreCase("record-send-rate") && group.equalsIgnoreCase("producer-metrics")) {
                    recordSendRate += entry.getValue().value();
                }
            }
        }
        System.out.println("Current Record Send Rate is: " + recordSendRate);
    }
}
