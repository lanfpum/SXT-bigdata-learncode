package lxpsee.top.transformaer.utils;

import lxpsee.top.transformaer.common.GlobalConstants;
import lxpsee.top.transformaer.utils.ip.IPSeeker;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/19 16:02.
 * <p>
 * Ip解析工具类
 */
public class IPSeekerExt extends IPSeeker {
    /**
     * 保存纯真ip库的ip文件路径
     */
    private static final String ipFilePath = "ip/qqwry.dat";

    /**
     * 静态的代码解析类对象，用于单例模式
     */
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt(ipFilePath);

    /**
     * 构造函数，private修饰，单例模式
     * 调用父类
     *
     * @param ipFilePath
     */
    private IPSeekerExt(String ipFilePath) {
        super(ipFilePath);
    }

    public static IPSeekerExt getInstance() {
        return ipSeekerExt;
    }

    public RegionInfo analysisIp(String ip) {
        RegionInfo regionInfo = new RegionInfo();

        if (null != ip && !"".equals(ip.trim())) {
            String country = super.getCountry(ip);
        }

        return null;
    }

    /**
     * 地域描述信息内部类
     * 属性均为默认值：未知
     */
    public static class RegionInfo {
        private String country = GlobalConstants.DEFAULT_VALUE;
        private String province = GlobalConstants.DEFAULT_VALUE;
        private String city = GlobalConstants.DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    "country='" + country +
                    ", province='" + province +
                    ", city='" + city +
                    '}';
        }
    }

}
