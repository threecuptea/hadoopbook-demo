package com.myspace.hadoopbook.counter;

import com.myspace.hadoopbook.JobBuilder;
import com.myspace.hadoopbook.NcdcRecordParser;
import com.myspace.hadoopbook.basic.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 12/10/13
 * Time: 10:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureWithCounters2 extends Configured implements Tool {

    enum Temperature {
        MISSING,
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
            }
            else if (parser.isMissingTemperature()) {
                System.out.println("Ignoring possible corrupt input: "+value);
                context.getCounter(Temperature.MISSING).increment(1);
            } else if (parser.isMalformedTemperature()) {
                context.getCounter(Temperature.MALFORMED).increment(1);
            }

            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job =  JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null)
            return -1;
        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass((MaxTemperatureReducer.class));
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new MaxTemperatureWithCounters2(), args);
        System.exit(exit);

    }
}