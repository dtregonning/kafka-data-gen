package generator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * MetricsCalculator is a class responsible for producing consolidated metric counts from multiple threads and
 * returning a string and metric based on the parameter sent through.

 *
 * @author      Don Tregonning (dtregonning)
 * @version     1.0
 * @since       1.0
 */
public class MetricsCalculator {
    /** Single Log4J Logger for class. */
    private static Logger logger = LogManager.getLogger(EPSThread.class);

    /** Array of Kafka Producers to track and return metrics for. */
    protected static Producer<String, String>[] producerArray;

    /** Current index value for the producerArray */
    private static int producerArrayIndex = 0;

    /**
     * Constructor implements the static array producerArray with the correct size based on the configurable
     * commandline parameter CommandLineParams.workerThreadCount.
     *
     * @param workerThreadCount       the amount of worker threads configured within kafka-data-gen

     * @see             Producer
     * @since           1.0
     */
    public MetricsCalculator(int workerThreadCount) {
        producerArray = (Producer<String, String>[])new Producer[workerThreadCount];
    }

    /**
     * addProducer will add a Kafka Producer to the class array producerArray. Adding producers to this
     * array is used as a central lookup of the current producers inside of worker threads.
     *
     * @param  producer the amount of worker threads configured within kafka-data-gen
     * @return          <code>true</code>if the producer is successfully added to array.
     *                  <code>false</code> otherwise.
     *
     * @since           1.0
     */
    public synchronized boolean addProducer(Producer<String, String> producer) {
        try {
            producerArray[producerArrayIndex] = producer;
            producerArrayIndex++;
        } catch (Exception Ex) {
            logger.error(Ex.toString());
            return false;
        }
          return true;
    }
    /**
     * method used for fetching metrics from the following
     * https://docs.confluent.io/current/kafka/monitoring.html#producer-metrics.
     *
     *
     *
     * @param   metricName  Name of metric to
     * @param   metricGroup Group of metric
     * @return              double value consolidatedMetricValue which will bring together metric value from all
     *                      worker threads and return a single value.
     *
     * @since               1.0
     */
    public double getKafkaProducerMetrics(String metricName, String metricGroup) {
        double consolidatedMetricValue = 0;
        for(int i = 0; i < producerArrayIndex; i++){
            Producer<String, String> producer = producerArray[i];
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                String name = entry.getKey().name();
                String group = entry.getKey().group();
                if (name.equalsIgnoreCase(metricName) && group.equalsIgnoreCase(metricGroup)) {
                    consolidatedMetricValue += entry.getValue().value();
                }
            }
        }
        return consolidatedMetricValue;
    }
}
