package com.myspace.hadoopbook.etl;


import com.myspace.hadoopbook.TextIntPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/23/14
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class EtlReducer extends Reducer<TextIntPair, PreferRejectWritable, Text, Text> {

    static final char OUT_FIELD_SEP = '\t';   //The separator between key and value is "\t", Using the same separator among fields in value.

     //Got into the following issues:
    //-3lppfJv8IQ	53	54	37	37	train
   // -3lppfJv8IQ	3	3	2	0	test
    //-3lppfJv8IQ	6	0	3	3	test
    //http://stackoverflow.com/questions/19589552/java-hadoop-reducer-receives-different-values-for-the-same-key-multiple-times
    //The Combiner may be called 0, 1, or many times on each key between the mapper and reducer,  I should not assume anything, FIXED it.

    @Override
    protected void reduce(TextIntPair key, Iterable<PreferRejectWritable> values, Context context) throws IOException, InterruptedException {
        //Since the default separator between key and value is '\t',  I keep it consistent so that I can load external table in Hive
        int currSourceOrd = -1;
        PreferRejectWritable currWritable = null;
        //Key is grouped using EtlDriver.GroupComparator (id ONLY).  That ensure that I would retrieve once per itemId
        //Key is sorted using EtlDriver.KeyComparator (id, then source), therefore values should be in this order too (SecondarySort).  There can be
        //multiple records for the same itemId and source.  Need to consolidate it.

        for (PreferRejectWritable value: values) {
            if (value.getSourceOrd() != currSourceOrd ) {
                if (currWritable != null)
                    outputSummary(key.getId(), currWritable, context);
                currSourceOrd = value.getSourceOrd();
                currWritable = value.clone();
            }
            else currWritable.incrementBy(value);
        }
        //Write out the last batch but prevent empty iterator too
        if (currWritable != null)
            outputSummary(key.getId(), currWritable, context);

    }

    private void outputSummary(Text id, PreferRejectWritable currWritable, Context context) throws IOException, InterruptedException {
        String combinedValue = new StringBuilder()
                .append(currWritable.getLeftPreferred()).append(OUT_FIELD_SEP)
                .append(currWritable.getRightPreferred()).append(OUT_FIELD_SEP)
                .append(currWritable.getLeftRejected()).append(OUT_FIELD_SEP)
                .append(currWritable.getRightRejected()).append(OUT_FIELD_SEP)
                .append(PreferRejectWritable.PreferenceSource.values()[currWritable.getSourceOrd()]).toString();

        context.write(id, new Text(combinedValue));// one record per (one item + one source)
    }

}
