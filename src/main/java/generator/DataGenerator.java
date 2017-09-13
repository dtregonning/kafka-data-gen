package generator;

import static java.nio.file.StandardOpenOption.*;
import java.nio.file.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import org.kohsuke.args4j.Option;
/**
 * Created by dtregonning on 9/12/17.
 */
public class DataGenerator {
    public static void main(String[] args) {
        int message_count = Integer.parseInt(args[0]);
        int message_size = Integer.parseInt(args[1]);
        long message_delay = Long.parseLong(args[2]);

        for (int i = 0; i < message_count; i++) {
            DataGenMessage message = new DataGenMessage(message_size);
            String s = message.toJSON();
            byte data[] = s.getBytes();
            Path p = Paths.get("./logfile.txt");

            try (OutputStream out = new BufferedOutputStream(
                    Files.newOutputStream(p, CREATE, APPEND))) {
                out.write(data, 0, data.length);
            } catch (IOException x) {
                System.err.println(x);
            }
            try {
                TimeUnit.SECONDS.sleep(message_delay);
            } catch (InterruptedException x) {
                System.err.println(x);
            }
        }
    }
}
