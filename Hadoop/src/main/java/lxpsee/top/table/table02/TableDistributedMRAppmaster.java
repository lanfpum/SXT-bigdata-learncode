package lxpsee.top.table.table02;

import lxpsee.top.mrconstant.Constant;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.net.URI;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 11:59.
 * <p>
 * D:\workDir\otherFile\temp\2019-1\table\data\order.txt D:\workDir\otherFile\temp\2019-1\table\output2
 */
public class TableDistributedMRAppmaster {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(TableDistributedMRAppmaster.class);
        job.setMapperClass(DistributedCacheMapper.class);
//        job.setReducerClass(DistributedCacheReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI(Constant.PRODUCT_TABLE_PATH));
        job.setNumReduceTasks(0);

        File outputDir = new File(args[1]);

        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }

        FileInputFormat.setInputPaths(job, new Path(Constant.LOCAL_PATH_FILE_PRE + args[0]));
        FileOutputFormat.setOutputPath(job, new Path(Constant.LOCAL_PATH_FILE_PRE + args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
