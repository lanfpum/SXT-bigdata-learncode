package lxpsee.top.outputformat;

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
 * Created by 努力常态化 on 2019/1/21 22:40.
 */
public class FilterMRAppMaster {
    public static void main(String[] args) throws Exception {
        args = new String[]{
                "D:/workDir/otherFile/temp/2019-1/outputformat/data/log.txt",
                "D:/workDir/otherFile/temp/2019-1/outputformat/output1"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FilterMRAppMaster.class);
        job.setMapperClass(FilterMapper.class);
//        job.setReducerClass(FilterReducer.class);

        job.setOutputFormatClass(FilterOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
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
