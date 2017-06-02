package com.myspace.hadoopbook.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 11/26/13
 * Time: 12:11 PM
 * To change this template use File | Settings | File Templates.
 */
//Need to unmask.  It's required 022 instead of 002, setfacl -d -m g::rx /tmp/dfs/data
//Need to overcome the issue: Invalid directory in dfs.data.dir: Incorrect permission for /tmp/dfs/data/data1, expected: rwxr-xr-x, while actual: rwxrwxr-x

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
 *
 * I ends up add umask 022 to .basrc
 */
public class MaxTemperatureClusterTest extends ClusterMapReduceTestCase {

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
        //System.setProperty("dfs.datanode.data.dir.perm", "775");
        super.setUp();
    }

    @Test
    public void test() throws Exception  {
        Configuration conf = this.createJobConf();
        Path input = this.getInputDir();
        Path output = this.getOutputDir();

        Path inputLocal = new Path("input/ncdc/micro");
        FileSystem fs = this.getFileSystem();
        fs.copyFromLocalFile(inputLocal, input);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exit = driver.run(new String[]{input.toString(), output.toString()});
        assertThat(exit, is(0));

        FileStatus[] statuses = fs.listStatus(output, new MaxTemperatureLocalTest.LogPathFilter());
        Path[] paths = FileUtil.stat2Paths(statuses);
        assertThat(paths.length, equalTo(1));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(paths[0])));
        assertThat(reader.readLine(), Matchers.is("1949\t111"));
        assertThat(reader.readLine(), Matchers.is("1950\t22"));
        assertThat(reader.readLine(), nullValue());


    }


}
