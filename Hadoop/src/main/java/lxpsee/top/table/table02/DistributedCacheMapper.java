package lxpsee.top.table.table02;

import lxpsee.top.mrconstant.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 16:35.
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final String PRODUCT_TABLE_NAME = "product.txt";
    Map<String, String> productMap = new HashMap<String, String>();

    Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(PRODUCT_TABLE_NAME)));
        String line;

        while (StringUtils.isNotBlank(line = reader.readLine())) {
            String[] fields = line.split(",");
            productMap.put(fields[0], fields[1]);
        }

        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        fields[1] = productMap.get(fields[1]);
        text.set(fields[0] + "\t" + fields[1] + "\t" + fields[2]);
        context.write(text, NullWritable.get());
    }
}
