package com.myspace.hadoopbook.join;

import com.myspace.hadoopbook.JobBuilder;
import com.myspace.hadoopbook.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 4:19 PM
 *
 * command line:
 * hadoop jar hadoopbook-demo-1.0.jar com.myspace.hadoopbook.join.JoinDriver
 * input/ncdc/all input/ncdc/metadata output-join
 */
public class JoinDriver extends Configured implements Tool {


    static class KeyPartitioner extends Partitioner<TextPair, Text> {

        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }
        //Job job = Job.getInstance(getConf(), "Join weather records with station names");
        Job job = new Job(getConf(), "Join weather records with station names");
        job.setJarByClass(this.getClass());

        Path ncdcInput = new Path(args[0]);
        Path stationInput = new Path(args[1]);
        Path output = new Path(args[2]);

        JobBuilder.setLocalConfigurationAndCleanupOutput(job.getConfiguration(), output);

        //Input use MultipleInputs
        MultipleInputs.addInputPath(job, ncdcInput, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, stationInput, TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(KeyPartitioner.class); //control which reducer to go to
        job.setGroupingComparatorClass(TextPair.FirstComparator.class); //control which reduce record to go to.

        job.setMapOutputKeyClass(TextPair.class);
        job.setOutputKeyClass(Text.class);
        job.setReducerClass(JoinReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception{
        int exit = ToolRunner.run(new JoinDriver(), args);
        System.exit(exit);
    }
}
