package lxpsee.top.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/18 08:23.
 * 去掉双引号函数
 */
public class RemoveQuotesUDF extends UDF {
    public Text evaluate(Text str) {
        if (null == str) {
            return null;
        }

        if (StringUtils.isBlank(str.toString())) {
            return null;
        }

        return new Text(str.toString().replace("\"", ""));
    }

    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
    }
}
