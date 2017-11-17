package generator;

class EPSToken {
    private int tokenCount;

    public EPSToken() {
        tokenCount=0;
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

    public synchronized int getTokenCount() { return tokenCount; }
}