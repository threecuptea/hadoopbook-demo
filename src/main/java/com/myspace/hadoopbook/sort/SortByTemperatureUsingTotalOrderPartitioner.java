package com.myspace.hadoopbook.sort;


import com.myspace.hadoopbook.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/5/13
 * Time: 6:24 PM
 *
 * command line:
 * hadoop jar hadoopbook-demo-1.0.jar com.myspace.hadoopbook.sort.SortByTemperatureUsingTotalOrderPartitioner \
 * -D mapred.reduce.tasks=30 input/ncdc/all-seq output-totalsort
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        Configuration conf = job.getConfiguration();
        //JobBuilder.setLocalConfigurationAndCleanupOutput(conf, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.setOutputKeyClass(LongWritable.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 2000, 2);
         //the probability that the key would be chosen.  Total sample size.  number of split sampled.
        InputSampler.writePartitionFile(job, sampler);

        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
        DistributedCache.addCacheFile(partitionUri, conf);
        DistributedCache.createSymlink(conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exit = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exit);
    }

}
