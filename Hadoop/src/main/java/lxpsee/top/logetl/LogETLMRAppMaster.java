package lxpsee.top.logetl;

import lxpsee.top.utils.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 22:40.
 */
public class LogETLMRAppMaster {
    public static void main(String[] args) {
        if (args.length <= 0) {
            args = new String[]{
                    "/user/lanp/weblog/20190214/2015082819",
                    "/user/hive/warehouse/lp_syllabus.db/track_log/date=20150828/hour=19"
            };
        }

        Tools.deleteFileInHDFS(args[1].substring(0, 60), args[1].substring(60));
        new LogETLMRAppMaster().run(args);
    }

    public void run(String[] args) {
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);

            job.setJarByClass(LogETLMRAppMaster.class);
            job.setMapperClass(ETLMapper.class);
            job.setReducerClass(ETLReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
