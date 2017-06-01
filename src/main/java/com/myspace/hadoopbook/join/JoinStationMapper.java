package com.myspace.hadoopbook.join;


import com.myspace.hadoopbook.NcdcStationMetadataParser;
import com.myspace.hadoopbook.TextPair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 3:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        context.write(new TextPair(parser.getStationId(), "0"), new Text(parser.getStationName()));
    }
}
