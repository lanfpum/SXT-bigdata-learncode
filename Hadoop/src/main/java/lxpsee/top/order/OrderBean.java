package lxpsee.top.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 08:44.
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private double price;

    public OrderBean() {
    }

    public void set(String orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        int result = this.orderId.compareTo(o.orderId);

        if (result == 0) {
            result = this.price > o.price ? -1 : 1;
        }

        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
