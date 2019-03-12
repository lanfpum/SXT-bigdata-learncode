import lxpsee.top.hbase.sina.weibo.code.Constant;
import org.junit.Test;


/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/7 17:32.
 */
public class TestEnum {
    @Test
    public void test() {
       /* System.out.println(Constant.WEIBO_NAME_SPACE);
        System.out.println(Constant.WEIBO_CONTENT_TABLE.COLUMN_FAMILY_INFO);
        System.out.println(Constant.WEIBO_CONTENT_TABLE.COLUMN_FAMILY_INFO.getvalue());*/

        Constant.WEIBO_RELATIONS_TABLE[] values = Constant.WEIBO_RELATIONS_TABLE.values();

        for (Constant.WEIBO_RELATIONS_TABLE value : values) {
            System.out.println(value.toString());

        }

    }
}
