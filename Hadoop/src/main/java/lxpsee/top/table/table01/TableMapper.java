package lxpsee.top.table.table01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 10:26.
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    TableBean tableBean = new TableBean();
    Text      text      = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name = ((FileSplit) context.getInputSplit()).getPath().getName();
        String line = value.toString();
        String[] fields;

        if (name.startsWith("order")) {
            fields = line.split("\t");
            tableBean.set(fields[0], fields[1], "", "0", Integer.parseInt(fields[2]));
            text.set(fields[1]);
        }
        if (name.startsWith("product")) {
            fields = line.split(",");
            tableBean.set("", fields[0], fields[1], "1", 0);
            text.set(fields[0]);
        }

        context.write(text, tableBean);
    }
}
