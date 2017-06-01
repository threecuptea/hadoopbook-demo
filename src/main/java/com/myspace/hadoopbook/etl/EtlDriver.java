package com.myspace.hadoopbook.etl;



import com.myspace.hadoopbook.JobBuilder;
import com.myspace.hadoopbook.TextIntPair;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/23/14
 * Time: 9:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class EtlDriver extends Configured implements Tool {

    static class IdPartitioner extends Partitioner<TextIntPair, PreferRejectWritable> {
        @Override
        public int getPartition(TextIntPair key, PreferRejectWritable value, int numPartitions) {
            return (key.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    static class GroupComparator extends WritableComparator {
        public GroupComparator() {
            super(TextIntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextIntPair && b instanceof TextIntPair) {
                TextIntPair ap = (TextIntPair) a;
                TextIntPair bp = (TextIntPair) b;
                return ap.getId().compareTo(bp.getId());
            }
            return super.compare(a, b);
        }
    }

    static class KeyComparator extends WritableComparator {
        public KeyComparator() {
            super(TextIntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextIntPair && b instanceof TextIntPair) {
                TextIntPair ap = (TextIntPair)a;
                TextIntPair bp = (TextIntPair)b;
                int cmp = ap.getId().compareTo(bp.getId());
                if (cmp != 0 )
                    return cmp;
                return TextIntPair.compare(ap.getOrder(), bp.getOrder());
            }
            return super.compare(a, b);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args); //Convenient method
        if (job == null) {
            return -1;
        }

        //JobBuilder.setLocalConfigurationAndCleanupOutput(job.getConfiguration(), FileOutputFormat.getOutputPath(job));
        //Convenient method, parseInputAndOutput setOutputPath

        job.setMapperClass(EtlMapper.class);
        job.setCombinerClass(EtlCombiner.class);  //How do I ensure combiner definitely run
        job.setReducerClass(EtlMultiOutputReducer2.class);
        //job.setReducerClass(EtlReducer.class);
        job.setMapOutputKeyClass(TextIntPair.class);
        job.setMapOutputValueClass(PreferRejectWritable.class);
        job.setPartitionerClass(IdPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(1);
        /*
        MultipleOutputs.addNamedOutput(job, "prefer", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "json", TextOutputFormat.class,
                Text.class, Text.class);
        */
        MultipleOutputs.addNamedOutput(job, "prefer", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "json", TextOutputFormat.class,
                Text.class, NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception{
        int exit = ToolRunner.run(new EtlDriver(), args);
        //Normally I need to clean up _SUCCESS file so that Hive LOAD DATA would work. MultipleOutputs only have S_SUCCESS
        //in the parent level, I load data from sub (External table point to sub).  No need to clean up in cases of MultipleOutputs
        System.exit(exit);
    }


}
