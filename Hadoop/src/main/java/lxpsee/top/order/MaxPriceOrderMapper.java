package lxpsee.top.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 08:50.
 */
public class MaxPriceOrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    OrderBean order = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String orderId = fields[0];
        double price = Double.parseDouble(fields[2]);
        order.set(orderId, price);
        context.write(order, NullWritable.get());
    }
}
