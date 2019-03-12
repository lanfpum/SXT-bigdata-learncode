package lxpsee.top.table.table01;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 10:21.
 */
public class TableBean implements Writable {
    private String orderId;
    private String productId;       //产品id
    private String productName;     //产品名称

    private String flag;            //表的标记
    private int    amount;             //产品数量

    public TableBean() {
    }

    public void set(String orderId, String productId, String productName, String flag, int amount) {
        this.setOrderId(orderId);
        this.setProductId(productId);
        this.setProductName(productName);
        this.setFlag(flag);
        this.setAmount(amount);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeUTF(productName);
        out.writeUTF(flag);
        out.writeInt(amount);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.productName = in.readUTF();
        this.flag = in.readUTF();
        this.amount = in.readInt();
    }

    @Override
    public String toString() {
        return orderId + "\t" + productName + "\t" + amount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
}
