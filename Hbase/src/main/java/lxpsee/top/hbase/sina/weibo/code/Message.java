package lxpsee.top.hbase.sina.weibo.code;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/12 09:46.
 */
public class Message {
    private String uid;
    private String timestamp;
    private String content;

    public Message() {
    }

    public Message(String uid, String timestamp, String content) {
        this.uid = uid;
        this.timestamp = timestamp;
        this.content = content;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Message{" +
                "uid='" + uid + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
