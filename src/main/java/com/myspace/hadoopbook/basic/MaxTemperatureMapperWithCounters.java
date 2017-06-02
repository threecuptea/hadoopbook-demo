package com.myspace.hadoopbook.basic;

import com.myspace.hadoopbook.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/19/13
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {

    enum Temperature {
        MISSING,
        MALFORMED
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
        else if (parser.isMalformedTemperature()) {
            System.out.println("Ignoring possible corrupt input: "+value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }
        else if (parser.isMissingTemperature()) {
            context.getCounter(Temperature.MISSING).increment(1);
        }
        //Dynamic counter
        context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
}
