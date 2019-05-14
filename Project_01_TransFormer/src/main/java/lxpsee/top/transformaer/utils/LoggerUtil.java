package lxpsee.top.transformaer.utils;

import cz.mallat.uasparser.UserAgentInfo;
import lxpsee.top.transformaer.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 14:23.
 */
public class LoggerUtil {

    private static final Logger logger = Logger.getLogger(LoggerUtil.class);

    /**
     * 解析给定的日志行，如果解析成功返回一个有值的map集合，如果解析失败，返回一个empty集合
     * 1.serverTime就是对于的毫秒级的时间戳,-1解析服务器时间失败
     * 2.请求体中？的位置应该在 》 0 且并非最后一位
     *
     * @param logText
     * @return
     */
    public static Map<String, String> handleLogText(String logText) {
        Map<String, String> result = new HashMap<String, String>();

        if (StringUtils.isNotBlank(logText)) {
            String[] logArr = logText.trim().split(EventLogConstants.LOG_SEPARTIOR);

            if (logArr.length == 3) {
                String ip = logArr[0].trim();
                result.put(EventLogConstants.LOG_COLUMN_NAME_IP, ip);
                long serverTime = TimeUtil.parseNginxServerTime2Long(logArr[1].trim());

                if (serverTime != -1)
                    result.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(serverTime));

                String requestBody = logArr[2].trim();
                int index = requestBody.indexOf("?");

                if (index >= 0 && index != requestBody.length() - 1)
                    requestBody = requestBody.substring(index + 1);
                else requestBody = null;

                if (StringUtils.isNotBlank(requestBody)) {
                    handleRequestBody(result, requestBody);
                    // 补全ip地址
                    IPSeekerExt.RegionInfo regionInfo = IPSeekerExt.getInstance().analysisIp(result.get(EventLogConstants.LOG_COLUMN_NAME_IP));

                    if (null != regionInfo) {
                        result.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, regionInfo.getCountry());
                        result.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, regionInfo.getProvince());
                        result.put(EventLogConstants.LOG_COLUMN_NAME_CITY, regionInfo.getCity());
                    }

                    // 开始补全浏览器信息：浏览器名称，浏览器版本号，浏览器所在操作系统，浏览器所在操作系统的版本
                    UserAgentInfo uInfo = UserAgentUtil.analyticUserAgent(result.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT));

                    if (null != uInfo) {
                        result.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, uInfo.getUaFamily());
                        result.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, uInfo.getBrowserVersionInfo());
                        result.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, uInfo.getOsFamily());
                        result.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, uInfo.getOsName());
                    }

                } else {
                    logger.debug("请求参数为空:" + logText);
                    result.clear();
                }

            } else logger.debug("日志行内容格式不正确:" + logText);

        } else logger.debug("日志行内容为空，无法进行解析:" + logText);

        return result;
    }

    /**
     * 处理请求参数<br/>
     * 处理结果保存到参数clientInfo集合
     * <p>
     * 保存最终用户行为数据的map集合
     * 循环处理参数, parameter格式为: c_time=1450569596991, =只会出现一次
     * 使用utf8解码,添加到结果集合中
     *
     * @param requestBody 请求参数中，用户行为数据，格式为:
     *                    u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=
     *                    1450569596991&ver=1&en=e_l&pl=website&sdk=js&b_rst=1440*900&
     *                    u_ud=4B16B8BB-D6AA-4118-87F8-C58680D22657&b_iev=Mozilla%2F5.0%
     *                    20(Windows%20NT%205.1)%20AppleWebKit%2F537.36%20(KHTML%2C%
     *                    20like%20Gecko)%20Chrome%2F45.0.2454.101%20Safari%2F537.36&l=
     *                    zh-CN&bf_sid=33cbf257-3b11-4abd-ac70-c5fc47afb797_11177014
     */
    public static void handleRequestBody(Map<String, String> result, String requestBody) {
//        System.out.println(requestBody);
        requestBody = requestBody.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
//        System.out.println(requestBody);
        String[] parameters = requestBody.split("&");

        for (String parameter : parameters) {
            String[] params = parameter.split("=");
            String key, value;

            try {
                key = URLDecoder.decode(params[0].trim(), "utf-8");
                value = URLDecoder.decode(params[1].trim(), "utf-8");
/*
                while (key.contains("%")) value = URLDecoder.decode(key.trim(), "utf-8");
                while (value.contains("%")) value = URLDecoder.decode(value.trim(), "utf-8");*/
//                System.out.println(key + " - " + value);
                result.put(key, value);
            } catch (UnsupportedEncodingException e) {
                logger.debug("解码失败:" + parameter, e);
            }
        }
    }

}
