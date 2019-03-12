package lxpsee.top.hbase.mapred.mapred01;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/22 08:56.
 * <p>
 * 1.将fruit的name和color提取出来，相当于将每一行数据读取出来放入到Put对象中。 不可变
 * 2.遍历添加column行
 * 3.添加/克隆列族:info
 * 4.添加/克隆列：name 添加/克隆列:color
 * 5.向该列cell加入到put对象中
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        Cell[] cells = value.rawCells();

        for (Cell cell : cells) {

            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {

                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }

        }

        context.write(key, put);
    }
}
