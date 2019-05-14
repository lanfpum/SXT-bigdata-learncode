package lxpsee.top.transformaer.mr;

import lxpsee.top.transformaer.common.EventLogConstants;
import lxpsee.top.transformaer.common.GlobalConstants;
import lxpsee.top.transformaer.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/20 11:02.
 * <p>
 * system.exit（0）:正常退出，程序正常执行结束退出
 * system.exit(1):是非正常退出，就是说无论程序正在执行与否，都退出，
 * <p>
 * yarn jar /home/lanp/lpdataDir/testJar/ETL.jar lxpsee.top.transformaer.mr.AnalysisDataRunner -date 20151220
 */
public class AnalysisDataRunner implements Tool {
    private Configuration configuration = null;

    public static void main(String[] args) {
        if (args.length <= 0) {
            args = new String[]{"-date", "20151220"};
        }

        try {
            int resultCode = ToolRunner.run(new AnalysisDataRunner(), args);

            if (resultCode == 0) System.out.println("Success!");
            else System.out.println("Fail!");

            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 1.获取配置文件
     * 2.处理 时间参数，默认或不合法时间则直接使用昨天日期
     * 3.设置Job参数，Mapper参数设置，Reducer参数设置
     * 4.配置数据输入，设置输出到HBase的信息
     * 5.Job提交
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        this.processArgs(configuration, args);

        Job job = Job.getInstance(configuration, "LP—Event-ETL");
        job.setJarByClass(AnalysisDataRunner.class);

        job.setMapperClass(AnalysisDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        this.initJobInputPath(job);

        initHBaseOutPutConfig(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    /**
     * 处理时间参数，如果没有传递参数的话，默认清洗前一天的。
     * Job脚本如下： bin/yarn jar ETL.jar lxpsee.top.transformaer.mr.AnalysisDataRunner -date 20151220
     *
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;

        for (int i = 0; i < args.length; i++) {

            if ("-date".equals(args[i])) {
                date = args[i + 1];
                break;
            }
        }

        //默认清洗昨天的数据到HBase
        if (StringUtils.isBlank(date) || TimeUtil.isValidateRunningDate(date)) date = TimeUtil.getYesterday();

        //将要清洗的目标时间字符串保存到conf对象中
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 初始化Job数据输入目录
     * 1.获取配置文件
     * 2.获取要执行ETL的数据是哪一天的数据
     * 3.格式化文件路径,如：  2019/03/20  将hdfs文件路径和它拼接
     * 4.根据文件路径去初始化输入目录,如果目录存在，初始化fileSystem
     *
     * @param job
     * @throws IOException
     */
    private void initJobInputPath(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String date = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES);
        String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");

        if (GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/"))
            hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + hdfsPath;
        else hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + File.separator + hdfsPath;
//        else hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + "/" + hdfsPath;

        FileSystem fileSystem = FileSystem.get(configuration);
        Path inputPath = new Path(hdfsPath);

        if (fileSystem.exists(inputPath)) FileInputFormat.addInputPath(job, inputPath);
        else throw new RuntimeException("HDFS中该文件目录不存在：" + hdfsPath);
    }

    /**
     * 设置输出到HBase的一些操作选项
     * 1.根据配置文件获取要ETL的数据是哪一天
     * 2.格式化HBase后缀名 date:2019-03-20 tableNameSuffix:20190320  每天一个表
     * 3.构建表名，指定输出
     * 4.创建HBaseAdmin对象，创建表描述对象，添加列族，创建表(表存在删除表)
     *
     * @param job
     * @throws IOException
     */
    private void initHBaseOutPutConfig(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String date = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES);
        String tableNameSuffix = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
        String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

        TableMapReduceUtil.initTableReducerJob(tableName, null, job);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(EventLogConstants.EVENT_LOGS_FAMILY_NAME));

        if (hBaseAdmin.tableExists(tableName)) {
            if (hBaseAdmin.isTableEnabled(tableName)) hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        hBaseAdmin.createTable(tableDescriptor);
        hBaseAdmin.close();
    }
}
