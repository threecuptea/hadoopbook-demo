package com.myspace.hadoopbook.join;

import com.myspace.hadoopbook.NcdcRecordParser;
import com.myspace.hadoopbook.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 3:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        context.write(new TextPair(parser.getStationId(), "1"), value);
    }
}
