package lxpsee.top.logetl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/28 11:52.
 */
public class ETLMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if (fields.length < 30) {
            context.getCounter("Web Counter ", "Length < 30").increment(1);
            return;
        }

        String url = fields[1];

        if (StringUtils.isBlank(url)) {
            context.getCounter("Web Counter ", "Url is Blank").increment(1);
            return;
        }

        String provinceId = fields[23];

        if (StringUtils.isBlank(provinceId)) {
            context.getCounter("Web Counter ", "ProvinceId is Blank").increment(1L);
            return;
        }

        try {
            int provinceId_Int = Integer.parseInt(provinceId);
        } catch (NumberFormatException e) {
            context.getCounter("Web Counter ", "ProvinceId is unknow").increment(1L);
            return;
        }

        context.write(key, value);
    }
}
