package com.myspace.hadoopbook.join;

import com.myspace.hadoopbook.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 4:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();

        String stationName = iter.next().toString();
        String record = null;
        while (iter.hasNext()) {
            record = iter.next().toString();
            Text outValue = new Text(stationName+"\t"+record);
            context.write(key.getFirst(), outValue);
        }

    }
}
