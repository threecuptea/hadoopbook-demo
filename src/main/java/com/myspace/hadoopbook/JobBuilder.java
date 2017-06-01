package com.myspace.hadoopbook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/5/13
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */

//A lot of convenient methods
public class JobBuilder {

    public static Job parseInputAndOutput(Tool tool, Configuration conf,
                                          String[] args) throws IOException {

        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }
        Job job = new Job(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s %s\n",
                tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

    public static void setLocalConfigurationAndCleanupOutput(Configuration conf,
                                          Path output) throws IOException {

        conf.set("mapreduce.framework.name", "local");
        conf.set("yarn.resourcemanager.address", "local");
        conf.set("mapreduce.jobtracker.address", "local");

        //Switch locally, clean up output
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);
    }
}
