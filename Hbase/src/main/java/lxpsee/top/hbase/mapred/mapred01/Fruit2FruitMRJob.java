package lxpsee.top.hbase.mapred.mapred01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/22 09:09.
 */
public class Fruit2FruitMRJob extends Configured implements Tool {
    private final String SOURCE_TABLE_NAME = "lanpeng:fruit";

    private static final String   TARGET_TABLE_NAME   = "lanpeng:fruit_mr";
    private static final String[] TABLE_COLUMN_FAMILY = {"info"};

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        createTable(TARGET_TABLE_NAME, TABLE_COLUMN_FAMILY, configuration);
        int status = ToolRunner.run(configuration, new Fruit2FruitMRJob(), args);
        System.exit(status);
    }

    /**
     * 1.得到Configuration
     * 2.创建Job任务
     * 3.配置Job
     * 4.设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
     * 4.1 数据源的表名/scan扫描控制器/设置Mapper类/设置Mapper输出key类型/设置Mapper输出value值类型/设置给哪个JOB
     * 5.设置Reducer
     * 6.设置Reduce数量，最少1个
     *
     * @param strings
     * @return
     * @throws Exception
     */
    public int run(String[] strings) throws Exception {
        Configuration configuration = this.getConf();

        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRJob.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        TableMapReduceUtil.initTableMapperJob(
                SOURCE_TABLE_NAME,
                scan,
                ReadFruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                TARGET_TABLE_NAME,
                WriteFruitMRReducer.class,
                job
        );

        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);

        if (!isSuccess) {
            throw new IOException("Job running with error");
        }

        return isSuccess ? 0 : 1;
    }

    private static void createTable(String tableName, String[] columnFamily, Configuration configuration) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            boolean isTableExist = admin.tableExists(tableName);

            if (isTableExist) {
                System.out.println("表" + tableName + "已经存在！");
                return;
            }

            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
