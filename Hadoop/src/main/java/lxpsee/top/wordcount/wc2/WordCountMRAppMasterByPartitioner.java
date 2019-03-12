package lxpsee.top.wordcount.wc2;

import lxpsee.top.wordcount.wc1.WordCountMapper;
import lxpsee.top.wordcount.wc1.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/17 16:19.
 * * 相当于一个yarn集群的客户端，
 * * 需要在此封装我们的mr程序相关运行参数，指定jar包
 * * 最后提交给yarn
 * <p>
 * <p>
 * 1 获取配置信息，或者job对象实例
 * 2 指定本业务job要使用的mapper/Reducer业务类
 * 3 指定mapper输出数据的kv类型
 * 4 指定最终输出的数据的kv类型
 * 5 指定job的输入原始文件所在目录
 * 6 指定本程序的jar包所在的本地路径(在hdfs上执行时需要路径)
 * 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
 * 8 配置提交到yarn上运行,windows和Linux变量不一致
 *
 * file:///D:\workDir\otherFile\temp\2019-1\wc\testdata file:///D:\workDir\otherFile\temp\2019-1\wc\output2
 */
public class WordCountMRAppMasterByPartitioner {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountMRAppMasterByPartitioner.class);

        job.setPartitionerClass(WordCountPartitions.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
