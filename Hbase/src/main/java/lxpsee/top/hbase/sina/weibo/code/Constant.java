package lxpsee.top.hbase.sina.weibo.code;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/7 18:12.
 */
public interface Constant {
    //微博项目的名称空间，微博内容表的表名,用户关系表的表名,微博收件箱表的表名
    String WEIBO_NAME_SPACE = "weibo";
    String WEIBO_CONTENT_TABLE_NAME = "weibo:content";
    String WEIBO_RELATIONS_TABLE_NAME = "weibo:relations";
    String WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME = "weibo:receive_content_email";

    enum WEIBO_CONTENT_TABLE {
        COLUMN_FAMILY_INFO("info"),
        COLUMN_QUALIFIER_CONTENT("content"),
        COLUMN_QUALIFIER_TITLE("title"),
        COLUMN_QUALIFIER_PIC_URL("pic_url");

        private String value;

        WEIBO_CONTENT_TABLE(String value) {
            this.value = value;
        }

        public String getvalue() {
            return value;
        }
    }

    enum WEIBO_RELATIONS_TABLE {
        COLUMN_FAMILY_ATTENDS("attends"),
        COLUMN_FAMILY_FANS("fans");

        private String value;

        WEIBO_RELATIONS_TABLE(String value) {
            this.value = value;
        }

        public String getvalue() {
            return value;
        }
    }

    enum WEIBO_RECEIVE_CONTENT_EMAIL_TABLE {
        COLUMN_FAMILY_INFO("info");

        private String value;

        WEIBO_RECEIVE_CONTENT_EMAIL_TABLE(String value) {
            this.value = value;
        }

        public String getvalue() {
            return value;
        }
    }

}
