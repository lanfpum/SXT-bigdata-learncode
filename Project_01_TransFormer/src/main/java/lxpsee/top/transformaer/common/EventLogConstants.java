package lxpsee.top.transformaer.common;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 15:01.
 * <p>
 * 定义日志收集客户端收集得到的用户数据参数的name名称
 * 以及event_logs这张hbase表的结构信息
 * 用户数据参数的name名称就是event_logs的列名
 */
public class EventLogConstants {

    /**
     * 事件枚举类。指定事件的名称
     * final成员变量表示常量，只能被赋值一次，赋值后值不再改变。
     * 成员变量（注意是类的成员变量，局部变量只需要保证在使用之前被初始化赋值即可）必须在定义时或者构造器中进行初始化赋值
     */
    public static enum EventEnum {
        LAUNCH(1, "launch event", "e_l"),                   // launch事件，表示第一次访问
        PAGEVIEW(2, "page view event", "e_pv"),             // 页面浏览事件
        CHARGEREQUEST(3, "charge request event", "e_crt"),  // 订单生产事件
        CHARGESUCCESS(4, "charge success event", "e_cs"),   // 订单成功支付事件
        CHARGEREFUND(5, "charge refund event", "e_cr"),     // 订单退款事件
        EVENT(6, "event duration event", "e_e")             // 事件
        ;

        public final int id;        // id 唯一标识
        public final String name;   // 名称
        public final String alias;  // 别名，用于数据收集的简写

        private EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 获取匹配别名的event枚举对象，如果最终还是没有匹配的值，那么直接返回null。
         */
        public static EventEnum valueOfAlias(String alias) {

            for (EventEnum eventEnum : values()) {
                if (eventEnum.alias.equals(alias)) return eventEnum;
            }

            return null;
        }
    }

    /**
     * 平台名称常量类
     */
    public static class PlatformNameConstants {
        public static final String PC_WEBSITE_SDK = "website";
        public static final String JAVA_SERVER_SDK = "java_server";
    }

    /**
     * 当前url
     */
    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";

    /**
     * 订单id
     */
    public static final String LOG_COLUMN_NAME_ORDER_ID = "oid";

    /**
     * 订单货币类型
     */
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE = "cut";

    /**
     * 订单支付方式
     */
    public static final String LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE = "pt";

    /**
     * 订单金额
     */
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT = "cua";

    /**
     * category名称
     */
    public static final String LOG_COLUMN_NAME_EVENT_CATEGORY = "ca";

    /**
     * action名称
     */
    public static final String LOG_COLUMN_NAME_EVENT_ACTION = "ac";

    /**
     * 会员唯一标识符
     */
    public static final String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";

    /**
     * 定义platform
     */
    public static final String LOG_COLUMN_NAME_PLATFORM = "pl";

    /**
     * 事件名称
     */
    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";

    /**
     * event_logs表的列簇名称
     */
    public final static String EVENT_LOGS_FAMILY_NAME = "info";

    /**
     * event_logs表列簇对应的字节数组
     */
    public final static byte[] BYTES_EVENT_LOGS_FAMILY_NAME = Bytes.toBytes(EVENT_LOGS_FAMILY_NAME);

    /**
     * 日志分隔符
     */
    public static final String LOG_SEPARTIOR = "\\^A";

    /**
     * 用户ip地址
     */
    public static final String LOG_COLUMN_NAME_IP = "ip";

    /**
     * 服务器时间
     */
    public final static String LOG_COLUMN_NAME_SERVER_TIME = "server_time";

    /**
     * ip地址解析的所属国家
     */
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";

    /**
     * ip地址解析的所属省份
     */
    public static final String LOG_COLUMN_NAME_PROVINCE = "province";

    /**
     * ip地址解析的所属城市
     */
    public static final String LOG_COLUMN_NAME_CITY = "city";

    /**
     * 浏览器user agent参数
     */
    public static final String LOG_COLUMN_NAME_USER_AGENT = "b_iev";

    /**
     * 操作系统名称
     */
    public static final String LOG_COLUMN_NAME_OS_NAME = "os";

    /**
     * 操作系统版本
     */
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_v";

    /**
     * 浏览器名称
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser";

    /**
     * 浏览器版本
     */
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_v";

    /**
     * 用户唯一标识符
     */
    public static final String LOG_COLUMN_NAME_UUID = "u_ud";

    /**
     * 表名称
     */
    public static final String HBASE_NAME_EVENT_LOGS = "lanpeng:event-logs";
}
