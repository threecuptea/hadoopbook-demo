package com.myspace.hadoopbook.etl;

import com.myspace.hadoopbook.TextIntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/23/14
 * Time: 10:00 AM
 * To change this template use File | Settings | File Templates.
 */

public class EtlMapper extends Mapper<LongWritable, Text, TextIntPair, PreferRejectWritable> {
    /**
     * I use customized TextIntPair because I don't want sourceLabel dictate the order
     */

    private PreferRejectWritable.PreferenceSource source;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path path =  ((FileSplit) context.getInputSplit()).getPath();
        if (path.getName().endsWith("train")) {
            source = PreferRejectWritable.PreferenceSource.TRAIN;
        }
        else {
            source = PreferRejectWritable.PreferenceSource.TEST;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] cols = value.toString().split(",");
        int sourceOrd = source.ordinal();
        PreferRejectWritable leftWritable = new  PreferRejectWritable(sourceOrd);
        PreferRejectWritable rightWritable = new  PreferRejectWritable(sourceOrd);

        if (cols[2].equals("left")) {
            leftWritable.incLeftPreferred();
            rightWritable.incRightRejected();

        }
        else {
            leftWritable.incLeftRejected();
            rightWritable.incRightPreferred();
        }
        context.write(new TextIntPair(cols[0], sourceOrd), leftWritable);
        context.write(new TextIntPair(cols[1], sourceOrd), rightWritable);
    }

}
