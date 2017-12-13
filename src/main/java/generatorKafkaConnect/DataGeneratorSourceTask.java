package generatorKafkaConnect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class DataGeneratorSourceTask extends SourceTask {

    @Override public String version() {
        return "0.1";
    }

    @Override public void start(Map<String, String> props) {

    }

    @Override public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override public void stop() {

    }
}
