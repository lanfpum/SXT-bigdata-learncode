package lxpsee.top.wordcount.wc2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/17 17:36.
 * <p>
 * 单词首字母按照ASCII码奇偶分区
 */
public class WordCountPartitions extends Partitioner<Text, IntWritable> {
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String firOfWord = text.toString().substring(0, 1);
        int num = firOfWord.toCharArray()[0];

        if (num % 2 == 0) {
            return 1;
        }

        return 0;
    }
}
