package lxpsee.top.inputformat;

import lxpsee.top.mrconstant.Constant;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 18:53.
 */
public class InputFormatMRAppMaster {
    public static void main(String[] args) throws Exception {
        args = new String[]{
                "D:/workDir/otherFile/temp/2019-1/inputformat/data",
                "D:/workDir/otherFile/temp/2019-1/inputformat/output1"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(InputFormatMRAppMaster.class);
        job.setMapperClass(SequenceFileMapper.class);

        job.setInputFormatClass(WholeFileInputformat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

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
