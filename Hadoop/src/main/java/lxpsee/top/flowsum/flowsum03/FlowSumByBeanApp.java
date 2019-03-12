package lxpsee.top.flowsum.flowsum03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:33.
 * <p>
 * 对统计结果进行倒序排序
 * <p>
 * file:///D:\workDir\otherFile\temp\2019-1\phoneflow\output1\part-r-00000 file:///D:\workDir\otherFile\temp\2019-1\phoneflow\output3
 */
public class FlowSumByBeanApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSumByBeanApp.class);
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
     /*   Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
*/
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
