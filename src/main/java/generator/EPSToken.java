package generator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class EPSToken {
    private static Logger logger = LogManager.getLogger(EPSToken.class);
    private int tokenCount;
    private int messageKey;
    private boolean finished;

    public EPSToken() {
        tokenCount = 0;
        messageKey = 0;
        finished = false;
    }
    public synchronized void increaseTokens(int tokenNumber) {
        tokenCount += tokenNumber;
    }

    public synchronized boolean takeToken() {
        if(tokenCount != 0) {
            tokenCount--;
            return true;
        }
        return false;
    }

    public synchronized void toggleFinished() {
        finished = !finished;
    }

    public boolean complete() {
        if(getTokenCount() == 0 && getFinished() == true) { return true; }
        else {return false; }
    }

    public synchronized int getTokenCount() { return tokenCount; }
    public synchronized int getKey() {
        if(messageKey % 10000 == 0) {
            logger.info(getMessageKey() + " events created and shipped");
        }

    return ++messageKey; }
    public synchronized int getMessageKey() { return messageKey; }
    public synchronized boolean getFinished() { return finished; }
}