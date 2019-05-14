package lxpsee.top.sparkBookLearn.javaVersion.javaBean;

import java.io.Serializable;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/15 08:52.
 */
public class AvgCount implements Serializable {
    private int total;
    private int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return total / (double) num;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
