package com.myspace.hadoopbook.etl;

import com.myspace.hadoopbook.TextIntPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 2/1/14
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class EtlMultiOutputReducer2 extends Reducer<TextIntPair, PreferRejectWritable, Text, Text> {

    static final char OUT_FIELD_SEP = '\t';    //The separator between key and value is "\t", Using the same separator among fields in value.

    static final String baseUrl = "https://www.googleapis.com/youtube/v3/videos?part=topicDetails,statistics&id=%s&key=AIzaSyDoJvvAj_1fiZWajF24I635VgJvdSHIQO0";

    private CloseableHttpClient client;
    private MultipleOutputs<Text, Text> multipleOutputs;
    static final String PREFER_BASE_PATH = "prefer/part";
    static final String JSON_BASE_PATH = "json/part";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        client = HttpClients.createDefault();
        multipleOutputs = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
        client.close();
        multipleOutputs.close();
    }
    //Got into the following issues:
    //-3lppfJv8IQ	53	54	37	37	train
    //-3lppfJv8IQ	3	3	2	0	test
    //-3lppfJv8IQ	6	0	3	3	test
    //http://stackoverflow.com/questions/19589552/java-hadoop-reducer-receives-different-values-for-the-same-key-multiple-times
    //The Combiner may be called 0, 1, or many times on each key between the mapper and reducer,  I should not assume anything. FIXED it.

    @Override
    protected void reduce(TextIntPair key, Iterable<PreferRejectWritable> values, Context context) throws IOException, InterruptedException {
        //Since the default separator between key and value is '\t',  I keep it consistent so that I can load external table in Hive
        int currSourceOrd = -1;
        PreferRejectWritable currWritable = null;
        //Key is grouped using EtlDriver.GroupComparator (id ONLY).  That ensure that I would retrieve item only once via HttpClinet
        //Key is sorted using EtlDriver.KeyComparator (id, then source), therefore values should be in this order too (SecondarySort).  There can be
        //multiple records for the same itemId and source.  Need to consolidate it.

        for (PreferRejectWritable value: values) {
            if (value.getSourceOrd() != currSourceOrd ) {
                if (currWritable != null)
                    outputSummary(key.getId(), currWritable);
                currSourceOrd = value.getSourceOrd();
                currWritable = value.clone();
            }
            else currWritable.incrementBy(value);
        }
        //Write out the last batch but prevent empty iterator too
        if (currWritable != null)
            outputSummary(key.getId(), currWritable);
        retrieveVideoMetaAndOutput(key.getId());
    }

    private void outputSummary(Text id, PreferRejectWritable currWritable) throws IOException, InterruptedException {
        String combinedValue = new StringBuilder()
                .append(currWritable.getLeftPreferred()).append(OUT_FIELD_SEP)
                .append(currWritable.getRightPreferred()).append(OUT_FIELD_SEP)
                .append(currWritable.getLeftRejected()).append(OUT_FIELD_SEP)
                .append(currWritable.getRightRejected()).append(OUT_FIELD_SEP)
                .append(PreferRejectWritable.PreferenceSource.values()[currWritable.getSourceOrd()]).toString();
        //The default delimiter between key and value is \t
        multipleOutputs.write("prefer", id, new Text(combinedValue), PREFER_BASE_PATH);
    }

    private void retrieveVideoMetaAndOutput(Text id) throws IOException, InterruptedException {
        String url = String.format(baseUrl, id.toString());
        HttpGet request = new HttpGet(url);
        CloseableHttpResponse response = client.execute(request);
        try {
            if (response.getStatusLine().getStatusCode() == 200) {
                BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                StringBuffer result = new StringBuffer();
                String line = "";
                while ((line = rd.readLine()) != null) {
                    result.append(line);
                }
                multipleOutputs.write("json", new Text(result.toString()), NullWritable.get(), JSON_BASE_PATH);
            }
        } finally {
            if (response != null)
                response.close();
        }
    }

    /*
    {
        "kind": "youtube#videoListResponse",
            "etag": "\"qQvmwbutd8GSt4eS4lhnzoWBZs0/lgLrW6R_R1vUNvWRDExil24EwLQ\"",
            "pageInfo": {
        "totalResults": 1,
                "resultsPerPage": 1
    },
        "items": [
        {
            "kind": "youtube#video",
                "etag": "\"qQvmwbutd8GSt4eS4lhnzoWBZs0/VA-KaHPR_WCYRoWWU64MwR6UUmY\"",
                "id": "wHkPb68dxEw",
                "statistics": {
            "viewCount": "9187",
                    "likeCount": "78",
                    "dislikeCount": "11",
                    "favoriteCount": "0",
                    "commentCount": "29"
        },
            "topicDetails": {
            "topicIds": [
            "/m/02mjmr"
            ],
            "relevantTopicIds": [
            "/m/0cnfvd",
                    "/m/01jdpf"
            ]
        }
        }
        ]
    }
    */
}
