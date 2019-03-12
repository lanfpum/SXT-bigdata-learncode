package lxpsee.top.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/18 08:03.
 * <p>
 * "31/Aug/2015:00:04:37 +0800"
 * 转换成
 * 2015-08-31 00:04:37
 */
public class DataTransformUDF extends UDF {
    private final SimpleDateFormat inputFormat  = new SimpleDateFormat("dd/MMM/yy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Text evaluate(Text str) {
        Text outputText = new Text();

        if (null == str) {
            return null;
        }

        if (StringUtils.isBlank(str.toString())) {
            return null;
        }

        try {
            Date parseDate = inputFormat.parse(str.toString().trim());
            String outputDate = outputFormat.format(parseDate);
            outputText.set(outputDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return outputText;
    }

    public static void main(String[] args) {
        System.out.println(new DataTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
    }
}
