package com.myspace.hadoopbook.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 11/26/13
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureLocalTest {

    static class LogPathFilter implements PathFilter        {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    }

    @Test
    public void test() throws Exception  {
        Configuration conf = new Configuration();
        conf.setStrings("mapreduce.framework.name", "local");
        conf.setStrings("yarn.resourcemanager.address", "local");
        conf.setStrings("mapreduce.jobtracker.address", "local");

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        FileSystem fs = FileSystem.getLocal(conf);
        Path input = new Path("input/ncdc/micro");
        Path output = new Path("output/ncdc/micro");
        fs.delete(output, true); //recursive = true

        int exit = driver.run(new String[]{input.toString(), output.toString()});
        assertThat(exit, is(0));

        checkOutput(conf, output);
    }

    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        FileStatus[] fileStatuses =   fs.listStatus(output, new LogPathFilter());
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        assertThat(paths.length, is(1));
        //FSDataInputStream
        BufferedReader actual = new BufferedReader(new InputStreamReader(fs.open(paths[0])));
        BufferedReader expected = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/expected.txt")));
        String expectedLine = null;
        while ((expectedLine = expected.readLine()) != null) {
          assertThat(actual.readLine(), is(expectedLine));
        }

        assertThat(actual.readLine(), nullValue());
        actual.close();
        expected.close();
    }

}
