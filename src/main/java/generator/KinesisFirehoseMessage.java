package generator;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class KinesisFirehoseMessage {
    private String thread;
    private String time;
    private String timing;
    private KinesisFirehoseMessageTimer timer;
    private String shardId;
    private String program;
    private String marketplace;
    private String startTime;
    private String endTime;
    private String host;
    private String streamName;
    private String operation;
    private String myID;
    private final String[] awsRegions =
        new String[]{"us-east-1","us-east-2","us-west-2","eu-west-1","eu-central-1","ap-northeast-1"};

    public KinesisFirehoseMessage() {
        Random r = new Random();



        thread = "pool-" + (r.nextInt(9-0) + 0) + "-thread-" + (r.nextInt(1000-0) + 0);
        time = (r.nextInt(9999-0) + 0) + "ms";
        timing = "MillisBehindLatest:0/1,FirehoseToS3.HintedBufferInterval:300000/1,UnknownSubOperationTime:1035/1,FirehoseToS3.ProcessRecords.Time:0/1,KinesisDataFetcher.getRecords.Time:34/1,FirehoseToS3.UsedBufferInterval:300000/1";
        timer = new KinesisFirehoseMessageTimer();
        shardId = "shardId-00000000000" + (r.nextInt(9-0) + 0);
        program = "AWSFirehoseToS3App";
        marketplace = "AWSFirehoseToS3App:perfgamma:" + awsRegions[r.nextInt((awsRegions.length -1) - 0) + 0];
        startTime = "1460404880.168";
        endTime = "Mon, 11 Apr 2016 20:01:20 UTC";
        host = (r.nextInt(5000-0) + 0) + "@ip-10-0-192-" +  (r.nextInt(256-0) + 0);
        streamName = "406183408373_durabilityfirehosetoredshiftcanary_1441486183619";
        operation = "ProcessTask";
        myID = "44113";
    }

    public String getThread() {
        return thread;
    }

    public String getTime() {
        return time;
    }

    public String getTiming() {
        return timing;
    }

    public String getShardId() {
        return shardId;
    }
    public String getProgram() {
        return program;
    }

    public String getMarketplace() {
        return marketplace;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getHost() {
        return host;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getOperation() {
        return operation;
    }

    public String getMyID() {
        return myID;
    }



    public String toString() {
    return "{\"Thread\":\"" + this.getThread() + "\"," +
                          "\"Time\":\"" + this.getTime() + "\"," +
                          "\"Timing\":\"" + this.getTiming() + "\"," +
                          timer + "," +
                          "\"ShardId\":\"" + this.getShardId() + "\"," +
                          "\"Program\":\"" + this.getProgram() + "\"," +
                          "\"Marketplace\":\"" + this.getMarketplace() + "\"," +
                          "\"StartTime\":\"" + this.getStartTime() + "\"," +
                          "\"EndTime\":\"" + this.getEndTime() + "\"," +
                          "\"Host\":\"" + this.getHost() + "\"," +
                          "\"StreamName\":\"" + this.getStreamName() + "\"," +
                          "\"Operation\":\"" + this.getOperation() + "\"," +
                          "\"MyId\":\"" + this.getMyID() + "\"}";
    }
}

class KinesisFirehoseMessageTimer {
    private Hashtable<String, Hashtable<String, Integer>> hashtable;

    public KinesisFirehoseMessageTimer() {
        hashtable = new Hashtable<>();
        hashtable.put("KinesisDataFetcher.getRecords.Time", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.CustomerFacingDataFreshness", randomizeRecordCountHasTable());
        hashtable.put("UnknownSubOperationTime", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.ValidTimeToDelivery.q5", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.ValidDataFreshness", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.OldestRecordTimestamp", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.ProcessRecords.Time", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.SuccessDataFreshness", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.DataFreshness", randomizeRecordCountHasTable());
        hashtable.put("MillisBehindLatest", randomizeRecordCountHasTable());
        hashtable.put("FirehoseToS3.GetOldestTimestamp.Time", randomizeRecordCountHasTable());

    }

    public Hashtable<String, Integer> randomizeRecordCountHasTable(){
        Hashtable<String, Integer> timeCount = new Hashtable<>();
        Random r = new Random();
        int Low = 0;
        int High = 100000;

        timeCount.put("count", 1);
        timeCount.put("time", r.nextInt(High-Low) + Low);

        return timeCount;
    }

    public String toString() {
        String returnString = "\"timer\": {";
        Set<String> keys = hashtable.keySet();
        Iterator<String> itr = keys.iterator();
        String topLevelKey;
        String subLevelKey;

        while(itr.hasNext()) {
            topLevelKey = itr.next();
            returnString += "\"" + topLevelKey + "\": {";

            Hashtable<String, Integer> subHashMap = hashtable.get(topLevelKey);

            Set<String> subLevelKeys = subHashMap.keySet();
            Iterator<String> itr2 = subLevelKeys.iterator();
            while(itr2.hasNext()) {
                subLevelKey = itr2.next();
                returnString += "\"" + subLevelKey + "\": \"" + subHashMap.get(subLevelKey) + "\"";
                if(itr2.hasNext()) {returnString += ",";}
                else {returnString += "}";}
            }

            if(itr.hasNext()) {returnString += ",";}
            else {returnString += "} ";}

        }
        return returnString;
    }
}

class KinesisTester {
    public static void main(String[] args) {
        KinesisFirehoseMessage KinesisFirehoseMessageTest = new KinesisFirehoseMessage();
        System.out.println(KinesisFirehoseMessageTest);
    }
 }
