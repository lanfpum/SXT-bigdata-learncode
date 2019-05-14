package lxpsee.top.transformaer.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 22:09.
 * <p>
 * 解析浏览器信息，解析useragent信息<br/>
 * 根据第三方jar文件:uasparser.jar进行解析<br/>
 */
public class UserAgentUtil {
    private static UASparser sparser = null;

    static {
        try {
            sparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {

        }
    }

    /**
     * 解析浏览器的user agent字符串，返回useragentinfo对象<br/>
     * 如果字符串为空，返回null，解析失败，也返回null
     *
     * @param userAgent
     * @return
     */
    public static UserAgentInfo analyticUserAgent(String userAgent) {

        if (StringUtils.isBlank(userAgent)) {
            return null;
        }

        UserAgentInfo info = null;

        try {
            info = sparser.parse(userAgent);
        } catch (IOException e) {

        }

        return info;
    }

}
