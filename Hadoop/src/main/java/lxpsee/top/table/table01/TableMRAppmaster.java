package lxpsee.top.table.table01;

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

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 11:59.
 * <p>
 * D:\workDir\otherFile\temp\2019-1\table\data D:\workDir\otherFile\temp\2019-1\table\output1
 */
public class TableMRAppmaster {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(TableMRAppmaster.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

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
