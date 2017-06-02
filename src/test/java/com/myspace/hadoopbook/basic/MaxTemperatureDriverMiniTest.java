package com.myspace.hadoopbook.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 12:54 PM
 * To change this template use File | Settings | File Templates.
 */

//Need to unmask.  It's required 022 instead of 002, setfacl -d -m g::rx /tmp/dfs/data

/**
 * Test case to run a MapReduce job.
 * <p/>
 * It runs a 2 node cluster Hadoop with a 2 node DFS and 2 tasktrackers.
 * <p/>
 * The JobConf to use must be obtained via the creatJobConf() method.
 * <p/>
 * It creates a temporary directory -accessible via getTestRootDir()-
 * for both input and output.
 * <p/>
 * The input directory is accesible via getInputDir() and the output
 * directory via getOutputDir()
 * <p/>
 * The DFS filesystem is formated before the testcase starts and after it ends.
 * Create MiniDFSCluster, MiniMRCluster
 */
public class MaxTemperatureDriverMiniTest extends ClusterMapReduceTestCase {

    static class OutputLogFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    }

    @Override
    protected void setUp() throws Exception {
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp");
        }
        if (System.getProperty("hadoop.log.dir") == null) {
            System.setProperty("hadoop.log.dir", "/tmp");
        }
        super.setUp();
    }

    @Test
    public void test() throws Exception  {
        Configuration conf = this.createJobConf();
        FileSystem fs = this.getFileSystem();
        Path input = this.getInputDir();
        Path output = this.getOutputDir();

        Path inputLocal = new Path("input/ncdc/micro");
        fs.copyFromLocalFile(inputLocal, input);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exit = driver.run(new String[]{input.toString(), output.toString()});
        assertThat(exit, is(0));

        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        assertThat(outputFiles.length, is(1));

        BufferedReader actual = new BufferedReader(new InputStreamReader(fs.open(outputFiles[0])));
        assertThat(actual.readLine(), is("1949\t111"));
        assertThat(actual.readLine(), is("1950\t22"));
        assertThat(actual.readLine(), nullValue());

        actual.close();
    }
}
