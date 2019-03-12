package lxpsee.top.order;

import lxpsee.top.mrconstant.Constant;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 09:05.
 * <p>
 * D:\workDir\otherFile\temp\2019-1\order\data D:\workDir\otherFile\temp\2019-1\order\output1
 */
public class MaxPriceOrderMRAppMaster {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MaxPriceOrderMRAppMaster.class);
        job.setPartitionerClass(MaxPriceOrderPartitoner.class);
        job.setGroupingComparatorClass(MaxPriceOrderGroupingComparator.class);
        job.setMapperClass(MaxPriceOrderMapper.class);
        job.setReducerClass(MaxPriceOrderReduce.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        File outputDir = new File(args[1]);

        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }

        FileInputFormat.setInputPaths(job, new Path(Constant.LOCAL_PATH_FILE_PRE + args[0]));
        FileOutputFormat.setOutputPath(job, new Path(Constant.LOCAL_PATH_FILE_PRE + args[1]));

        job.setNumReduceTasks(3);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
