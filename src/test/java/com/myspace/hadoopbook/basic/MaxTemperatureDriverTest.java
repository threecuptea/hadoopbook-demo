package com.myspace.hadoopbook.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 12:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTemperatureDriverTest {
    //I have Hadoop-yarn version, I  have to set those propertries, in order for yarn to know using LocalRunner
    /*
    hadoop com.myspace.hadoop.book.basic.MaxTemperatureDriver -fs file:/// -Dmapreduce.framework.name=local \
            -Dyarn.resourcemanager.address=local -Dmapreduce.jobtracker.address=local \
    input/ncdc/micro/ output
    */
    //Both mapred-site.xml and hadoop.conf under /etc/oozie have mapreduce.framework.name set to yarn
    static class OutputLogFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    }

    @Test
    public void test() throws Exception  {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("yarn.resourcemanager.address", "local");
        conf.set("mapreduce.jobtracker.address", "local");

        Path input = new Path("input/ncdc/micro");
        Path output = new Path("output/ncdc/micro");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exit = driver.run(new String[]{input.toString(), output.toString()});
        assertThat(exit, is(0));

        //checkOutput(conf, output);
    }

    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        //Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        //assertThat(outputFiles.length, is(1));
        assertThat(outputFiles.length, is(1));

        BufferedReader actual = new BufferedReader(new InputStreamReader(fs.open(outputFiles[0])));
        //Need to put under target/test-classes for Intellij to locate file
        BufferedReader expected = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/expected.txt"))) ;
        String expectedLine = null;
        while ((expectedLine = expected.readLine()) != null)  {
            assertThat(actual.readLine(), is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());

        actual.close();
        expected.close();
    }


}