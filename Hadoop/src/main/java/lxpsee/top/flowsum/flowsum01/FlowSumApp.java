package lxpsee.top.flowsum.flowsum01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:33.
 *
 * file:///D:\workDir\otherFile\temp\2019-1\phoneflow\data file:///D:\workDir\otherFile\temp\2019-1\phoneflow\output1
 */
public class FlowSumApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSumApp.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
