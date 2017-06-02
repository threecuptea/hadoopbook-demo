package com.myspace.hadoopbook.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 12:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureMapperTest {

    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                                      // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInputValue(value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperWithCounters())
                .withInputValue(value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .withCounters(counters)
                .runTest();
        Counter qCounter = counters.findCounter("TemperatureQuality", "1");
        assertThat(qCounter.getValue(), is(1L));
    }

    @Test
    public void processesPositiveTemperatureRecord() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+00111+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInputValue(value)
                .withOutput(new Text("1950"), new IntWritable(11))
                .runTest();                              // Temperature ^^^^^

    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99994+99999999999");
                                      // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInputValue(value)
                .runTest();
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperWithCounters())
                .withInputValue(value)
                .withCounters(counters)
                .runTest();
        Counter missingCounter = counters.findCounter(MaxTemperatureMapperWithCounters.Temperature.MISSING);
        assertThat(missingCounter.getValue(), is(1L));
        Counter qCounter = counters.findCounter("TemperatureQuality", "4");
        assertThat(qCounter.getValue(), is(1L));
    }

    @Test
    public void ignoresSuspectQualityRecord() throws IOException,
            InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+00112+99999999999");
                                      // Temperature ^^^^^
                                       // Suspect quality ^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInputValue(value)
                .runTest();
    }

    @Test
    public void parsesMalformedTemperature() throws IOException,
            InterruptedException {
        Text valueMal = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                // Year ^^^^
                "RJSN V02011359003150070356999999433201957010100005+353");
        // Temperature ^^^^^
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapperWithCounters())
                .withCounters(counters)
                .withInputValue(valueMal)
                .runTest();
        Counter malCounter = counters.findCounter(MaxTemperatureMapperWithCounters.Temperature.MALFORMED);
        assertThat(malCounter.getValue(), is(1L));
    }

}
