package com.myspace.hadoopbook.cache;

import com.myspace.hadoopbook.JobBuilder;
import com.myspace.hadoopbook.NcdcRecordParser;
import com.myspace.hadoopbook.NcdcStationMetadata;
import com.myspace.hadoopbook.basic.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/6/13
 * Time: 10:08 AM
 *
 * command line:
 * hadoop jar hadoopbook-demo-1.0.jar com.myspace.hadoopbook.sort.MaxTemperatureByStationNameUsingDistributedCacheFile \
 * -file input/ncdc/metadata/stations-fixed-width.tx input/ncdc/all output
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {

    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature())
                context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemperature()));
        }
    }

    static class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {
        private NcdcStationMetadata metadata;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            File file = new File("stations-fixed-width.txt");
            metadata.initialize(file);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            for (IntWritable value: values) {
                maxTemp = Math.max(maxTemp, value.get());
            }
            context.write(new Text(metadata.getStationName(key.toString())), new IntWritable(maxTemp));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(StationTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exit = ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
        System.exit(exit);
    }
}
