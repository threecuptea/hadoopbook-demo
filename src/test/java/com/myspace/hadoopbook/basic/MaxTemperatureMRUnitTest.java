package com.myspace.hadoopbook.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 11/26/13
 * Time: 10:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureMRUnitTest {

    @Test
    public void processesValidMapRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                                                   //xxxxx
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
        .withMapper(new MaxTemperatureMapperWithCounters())
        .withInputValue(value)
        .withCounters(counters)
        .withOutput(new Text("1950"), new IntWritable(-11))
        .runTest();

        Counter counter = counters.findCounter("TemperatureQuality", "1");
        assertThat(counter.getValue(), is(1L));
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99994+99999999999");
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperWithCounters())
                .withInputValue(value)
                .withCounters(counters)
                .runTest();
        Counter counter = counters.findCounter(MaxTemperatureMapperWithCounters.Temperature.MISSING);
        assertThat(counter.getValue(), is(1L));
    }

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
