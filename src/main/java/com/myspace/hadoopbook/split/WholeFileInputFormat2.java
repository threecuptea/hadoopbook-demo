package com.myspace.hadoopbook.split;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 12/11/13
 * Time: 12:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileInputFormat2 extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader2 recordReader = new WholeFileRecordReader2();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
