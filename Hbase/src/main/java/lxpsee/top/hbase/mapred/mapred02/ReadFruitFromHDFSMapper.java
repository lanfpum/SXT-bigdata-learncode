package lxpsee.top.hbase.mapred.mapred02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/25 08:29.
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String[] family    = {"info"};
    private String[] qualifier = {"name", "color"};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineSplit = line.split("\t");

        if (lineSplit.length < 3) {
            context.getCounter("HDFS Mapper", "data length is wrong").increment(1);
            return;
        }

        String rowKey = lineSplit[0];
        String name = lineSplit[1];
        String color = lineSplit[2];

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family[0]), Bytes.toBytes(qualifier[0]), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes(family[0]), Bytes.toBytes(qualifier[1]), Bytes.toBytes(color));

        context.write(immutableBytesWritable, put);
    }
}
