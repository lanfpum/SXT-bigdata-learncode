package lxpsee.top.transformaer.mr;

import lxpsee.top.transformaer.common.EventLogConstants;
import lxpsee.top.transformaer.common.EventLogConstants.EventEnum;
import lxpsee.top.transformaer.utils.LoggerUtil;
import lxpsee.top.transformaer.utils.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 14:49.
 */
public class AnalysisDataMapper extends Mapper<Object, Text, NullWritable, Put> {
    private static final Logger logger = Logger.getLogger(AnalysisDataMapper.class);

    private CRC32 crc32 = null;
    private byte[] family = null;
    private long currentDayInMills = -1;

    /**
     * 创建crc32对象，列族复制info，获取当天零点时刻毫秒值
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.crc32 = new CRC32();
        this.family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
        this.currentDayInMills = TimeUtil.getTodayInMillis();
    }

    /**
     * 1.将原始数据通过LoggerUtil解析成Map键值对，如果解析失败，则Map集合中无数据
     * 2.根据解析后的数据，生成对应的Event事件类型
     * 3.无法处理的事件，直接输出事件类型：处理具体的事件
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> clientInfo = LoggerUtil.handleLogText(value.toString());

        if (clientInfo.isEmpty()) {
            logger.debug("日志解析失败：" + value.toString());
            return;
        }

        EventEnum event = EventLogConstants.EventEnum.valueOfAlias(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));

        if (null == event)
            logger.debug("无法匹配对应的事件类型：" + clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
        else
            handleEventData(clientInfo, event, context, value);
    }

    /**
     * 处理具体的事件
     * 1.事件成功通过过滤，则处理事件,输出事件到HBase
     * 2.事件没有通过过滤
     *
     * @param clientInfo
     * @param event
     * @param context
     * @param value
     */
    private void handleEventData(Map<String, String> clientInfo, EventEnum event, Context context, Text value) throws IOException, InterruptedException {
        if (filterEventData(clientInfo, event)) outPutData(clientInfo, context);
        else logger.debug("事件格式不正确：" + value.toString());
    }

    /**
     * 过滤事件
     * 1.事件数据全局过滤,可以先进行事件数据全局过滤判断，再去 &&
     * 2.Java Server发来的数据
     * 2.1判断会员ID是否存在
     * 2.2判断属于哪个事件,退款事件不进行处理，支付成功事件则判断订单id是否存在,其他：输出原因并设为false
     * 3.pc端发来的数据
     * 3.1 下单事件：判断订单号，订单货币类型，订单支付方式，订单金额
     * 3.3Launch访问事件 退出
     * 3.4PV事件 当前的url不为空
     * <p>
     * 注意点，多个swith内嵌时候的break
     *
     * @param clientInfo
     * @param event
     * @return
     */
    public boolean filterEventData(Map<String, String> clientInfo, EventEnum event) {
        boolean result = StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM));

        switch (clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)) {

            case EventLogConstants.PlatformNameConstants.JAVA_SERVER_SDK:
                result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID));

                switch (event) {
                    case CHARGEREFUND:
                        break;
                    case CHARGESUCCESS:
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID));
                        break;
                    default:
                        logger.debug("无法处理指定事件：" + clientInfo);
                        result = false;
                        break;
                }
                break;

            case EventLogConstants.PlatformNameConstants.PC_WEBSITE_SDK:
                switch (event) {
                    case CHARGEREQUEST:
                        result = result
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT));
                        break;

                    case EVENT:
                        result = result
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION));
                        break;

                    case LAUNCH:
                        break;

                    case PAGEVIEW:
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
                        break;

                    default:
                        logger.debug("无法处理指定事件：" + clientInfo);
                        result = false;
                        break;
                }
                break;

            default:
                result = false;
                logger.debug("无法确定的数据来源：" + clientInfo);
                break;
        }

        return result;
    }

    /**
     * 输出事件到hbase
     * 1.取出用户唯一标识符，服务器时间
     * 2.浏览器信息已经解析完成，删除原始的浏览器信息
     * 3.创建rowkey
     * 4.创建put对象
     * 5.对解析后的集合进行遍历，添加到put对象中
     * 6.context写出
     *
     * @param clientInfo
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void outPutData(Map<String, String> clientInfo, Context context) throws IOException, InterruptedException {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        long serverTime = Long.valueOf(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));
        clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);

        byte[] rowkey = generateRowKey(uuid, serverTime, clientInfo);

        Put put = new Put(rowkey);

        for (Map.Entry<String, String> entry : clientInfo.entrySet()) {

            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue()))
                put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }

        context.write(NullWritable.get(), put);
    }

    /**
     * 生成rowkey方法
     * 1.清空crc32集合中的数据内容
     * 2.更新ccrc32中UUID的值和clientInfo的哈希码
     * 3.当前数据访问服务器的时间-当天00:00点的时间戳  8位数字 -- 4字节
     * 4.创建综合字节数组
     * 5.进行数组合并
     *
     * @param uuid       用户唯一标识符，U_UID
     * @param serverTime
     * @param clientInfo
     * @return
     */
    private byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo) {
        this.crc32.reset();

        if (StringUtils.isNotBlank(uuid)) this.crc32.update(Bytes.toBytes(uuid));

        this.crc32.update(Bytes.toBytes(clientInfo.hashCode()));

        byte[] timeBytes = Bytes.toBytes(serverTime - this.currentDayInMills);
        byte[] uuidAndMapDataBytes = Bytes.toBytes(this.crc32.getValue());

        byte[] buffer = new byte[timeBytes.length + uuidAndMapDataBytes.length];

        System.arraycopy(timeBytes, 0, buffer, 0, timeBytes.length);
        System.arraycopy(uuidAndMapDataBytes, 0, buffer, timeBytes.length, uuidAndMapDataBytes.length);
        return buffer;
    }


}
