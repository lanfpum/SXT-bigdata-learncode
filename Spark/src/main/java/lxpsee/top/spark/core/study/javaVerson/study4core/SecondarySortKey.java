package lxpsee.top.spark.core.study.javaVerson.study4core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 16:51.
 * 自定义的二次排序key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
    private Integer first;
    private Integer second;

    public int compare(SecondarySortKey that) {
        if (this.first - that.first != 0) return this.first - that.getFirst();
        else return this.second - that.getSecond();
    }

    public boolean $less(SecondarySortKey that) {
        if (this.first < that.getFirst()) return true;
        else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst()
                && this.second > that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst()
                && this.second == that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst()
                && this.second == that.getSecond()) {
            return true;
        }

        return false;
    }

    public int compareTo(SecondarySortKey that) {
        if (this.first - that.first != 0) return this.first - that.getFirst();
        else return this.second - that.getSecond();
    }

    public SecondarySortKey(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != null ? !first.equals(that.first) : that.first != null) return false;
        return second != null ? second.equals(that.second) : that.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }
}
