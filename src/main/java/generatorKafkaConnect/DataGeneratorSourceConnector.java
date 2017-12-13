package generatorKafkaConnect;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class DataGeneratorSourceConnector extends SourceConnector{
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";

    private String filename;
    private String topic;

    @Override public String version() {
        return AppInfoParser.getVersion();
    }

    @Override public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);

        if (topic == null || topic.isEmpty())
      //      throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(",")){}
        //    throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");

    }

    @Override public Class<? extends Task> taskClass() {
        return DataGeneratorSourceTask.class;
    }

    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        if (filename != null)
            config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override public void stop() {

    }

    @Override public ConfigDef config() {
        return null;
    }
}
