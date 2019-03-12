package lxpsee.top.flowsum.flowsum02;

import lxpsee.top.flowsum.flowsum01.FlowBean;
import lxpsee.top.flowsum.flowsum01.FlowSumMapper;
import lxpsee.top.flowsum.flowsum01.FlowSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:33.
 * <p>
 * 设置分区类，设置reducer的数量，应该和分区相对应
 * <p>
 * file:///D:\workDir\otherFile\temp\2019-1\phoneflow\data file:///D:\workDir\otherFile\temp\2019-1\phoneflow\output2
 */
public class FlowSumByPartitionerApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSumByPartitionerApp.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowSumpartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
