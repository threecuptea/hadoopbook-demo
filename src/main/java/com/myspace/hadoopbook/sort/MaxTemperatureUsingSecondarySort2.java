package com.myspace.hadoopbook.sort;

import com.myspace.hadoopbook.IntPair2;
import com.myspace.hadoopbook.JobBuilder;
import com.myspace.hadoopbook.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 12/11/13
 * Time: 10:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureUsingSecondarySort2 extends Configured implements Tool {

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair2, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature())
                context.write(new IntPair2(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
        }
    }

    static class  MaxTemperatureReducer extends Reducer<IntPair2, NullWritable, IntPair2, NullWritable> {
        @Override
        protected void reduce(IntPair2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    static class FirstPartitioner extends Partitioner<IntPair2, NullWritable> {
        @Override
        public int getPartition(IntPair2 key, NullWritable value, int numPartitions) {
            return (key.getFirst() & Integer.MAX_VALUE) % numPartitions;

        }
    }

    static class GroupComparator extends WritableComparator {
        public GroupComparator() {
            super(IntPair2.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof IntPair2 && b instanceof IntPair2) {
                IntPair2 ap = (IntPair2) a;
                IntPair2 bp = (IntPair2) b;
                return IntPair2.compare(ap.getFirst(), bp.getFirst());
            }
            return super.compare(a, b);
        }
    }

    static class KeyComparator extends WritableComparator {
        public KeyComparator() {
            super(IntPair2.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof IntPair2 && b instanceof IntPair2) {
                IntPair2 ap = (IntPair2) a;
                IntPair2 bp = (IntPair2) b;
                int cmp = IntPair2.compare(ap.getFirst(), bp.getFirst());
                if (cmp == 0)  {
                    cmp = - IntPair2.compare(ap.getSecond(), bp.getSecond());  //reverse
                }
                return cmp;
            }
            return super.compare(a, b);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair2.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(KeyComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new MaxTemperatureUsingSecondarySort2(), args);
        System.exit(exit);

    }


}
