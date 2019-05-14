package lxpsee.top.transformaer.utils;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 15:07.
 * <p>
 * 时间控制工具类
 */
public class TimeUtil {
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String HBASE_TABLE_NAME_SUFFIX_FORMAT = "yyyyMMdd";

    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     *
     * @param timeStr
     * @return
     */
    public static boolean isValidateRunningDate(String timeStr) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";

        if (null != timeStr && !timeStr.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(timeStr);
        }

        if (null != matcher) result = matcher.matches();

        return result;
    }

    /**
     * 获取当天的毫秒数 0:0:0:
     */
    public static long getTodayInMillis() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    /**
     * 将nginx服务器时间转换为时间戳，如果说解析失败，返回-1
     *
     * @param servertimeStr 格式: 1449410796.976
     * @return
     */
    public static long parseNginxServerTime2Long(String servertimeStr) {
        Date date = parseNginxServerTime2Date(servertimeStr);
        return date == null ? -1L : date.getTime();
    }

    /**
     * 将nginx服务器时间转换为date对象。如果解析失败，返回null *1000返回
     *
     * @param servertimeStr 格式: 1449410796.976
     * @return
     */
    private static Date parseNginxServerTime2Date(String servertimeStr) {
        if (StringUtils.isNotBlank(servertimeStr)) {
            long timestamp = Double.valueOf(Double.valueOf(servertimeStr.trim()) * 1000).longValue();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp);
            return calendar.getTime();
        }

        return null;
    }

    /**
     * 获取昨日的日期格式字符串数据 yyyy-MM-dd
     *
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }

    /**
     * 获取前一天对应格式的时间字符串
     *
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 将时间戳转换为指定格式的字符串
     *
     * @param input
     * @param pattern
     * @return
     */
    public static String parseLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
     *
     * @param input
     * @return
     */
    public static long parseString2Long(String input) {
        return parseString2Long(input, HBASE_TABLE_NAME_SUFFIX_FORMAT);
    }

    /**
     * 将指定格式的时间字符串转换为时间戳
     *
     * @param input
     * @param pattern
     * @return
     */
    public static long parseString2Long(String input, String pattern) {
        Date date = null;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }
}
