package com.myspace.hadoopbook.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 12/11/13
 * Time: 12:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileRecordReader2 extends RecordReader {

    private boolean processed;
    private FileSplit fileSplit;
    private BytesWritable value = new BytesWritable();
    private Configuration conf;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] buf = new byte[(int) fileSplit.getLength()];
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(path);
                IOUtils.readFully(in, buf, 0, buf.length);
                value.set(buf, 0, buf.length);
            }
            finally {
                if (in != null) {
                    IOUtils.closeStream(in);
                }
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (processed) return 1.0f; else return 0.0f;
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
