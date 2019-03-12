package lxpsee.top.hbase.mapred.mapred02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/25 08:43.
 *
 * hadoop jar hbase.jar lxpsee.top.hbase.mapred.mapred02.HDFS2HBaseDriver
 */
public class HDFS2HBaseDriver extends Configured implements Tool {
    private String[] params = {
            "test/fruit_hdfs.tsv",
//            "hdfs://lanpengcluster/user/lanp/test/fruit.txt",
            "lanpeng:fruit_hdfs"};

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new HDFS2HBaseDriver(), args);
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(HDFS2HBaseDriver.class);

        Path inputPath = new Path(params[0]);
        FileInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(params[1], WriteFruitMRFromTxtReducer.class, job);

        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);

        if (!isSuccess) {
            throw new IOException("Job running with error");
        }

        return isSuccess ? 0 : 1;
    }


}
