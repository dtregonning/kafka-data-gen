package generator;

import java.util.UUID;
import java.util.Random;

/**
 * Created by dtregonning on 9/12/17.
 */
public class DataGenMessage {
    String message = null;
    UUID uuid = null;

    public DataGenMessage(int size) {
        this.uuid = UUID.randomUUID();
        this.message = String.valueOf(randomizeMessage(size));
    }

    private char[] randomizeMessage(int size) {
        char[] char_array  = new char[size];
        Random r = new Random();
        for(int i = 0; i < char_array.length; i++) {
            char_array[i] = (char) (r.nextInt(26) + 'a');
        }
        return char_array;
    }

    public String toString() {
        return "UUID:" + this.uuid + "\nMESSAGE:" + this.message;
    }

    public String toJSON() {
        return "{"+ "\"uuid\":\"" + this.uuid +"\", \"message\":\"" + this.message + "\"}";
    }
}