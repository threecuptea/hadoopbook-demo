package com.myspace.hadoopbook.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTempertaureReducerTest {

    @Test
    public void returnsMaximumIntegerInValues() throws IOException,
            InterruptedException {

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
          .withReducer(new MaxTemperatureReducer())
          .withInputKey(new Text("1950"))
          .withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(20)))
          .withOutput(new Text("1950"), new IntWritable(20))
          .runTest();
    }



}
