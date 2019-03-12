package lxpsee.top.hbase.sina.weibo.code;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/7 16:26.
 */
public class WeiBo {

    // 获取配置对象
    private Configuration configuration = HBaseConfiguration.create();

    private void initWEIBOHbase() {
        initNameSpace();
        creatTableContent();
        createTableRelations();
        createTableReceiveContentEmails();
    }

    private void testPublishContent(WeiBo wb){
        wb.publishWEIBOContent("0001", "今天买了一包空气，送了点薯片，非常开心！！");
        wb.publishWEIBOContent("0001", "今天天气不错。");
    }

    private void testAddAttend(WeiBo wb){
        wb.publishWEIBOContent("0008", "准备下课！");
        wb.publishWEIBOContent("0009", "准备关机！");
        wb.addAttends("0001", "0008", "0009");
    }

    private void testRemoveAttend(WeiBo wb){
        wb.removeAttends("0001", "0008");
    }

    private void testShowMessage(WeiBo wb){
        List<Message> messages = wb.getAttendsContent("0001");
        for(Message message : messages){
            System.out.println(message);
        }
    }

    /**
     * 发布微博
     * a、微博内容表中数据+1
     * b、向微博收件箱表中加入微博的Rowkey
     */
    private void publishWEIBOContent(String uid, String content) {
        HConnection hConnection = null;

        try {
            hConnection = HConnectionManager.createConnection(configuration);

            // 微博内容表中添加1条数据
            HTableInterface contentTable = hConnection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT_TABLE_NAME));
            long timestamp = System.currentTimeMillis();
            String rowKeyOfContent = uid + "_" + timestamp;
            Put put = new Put(Bytes.toBytes(rowKeyOfContent));
            put.addColumn(Bytes.toBytes(Constant.WEIBO_CONTENT_TABLE.COLUMN_FAMILY_INFO.getvalue()),
                    Bytes.toBytes(Constant.WEIBO_CONTENT_TABLE.COLUMN_QUALIFIER_CONTENT.getvalue()),
                    timestamp, Bytes.toBytes(content));
            contentTable.put(put);

            // 向微博收件箱表中加入发布的Rowkey
            // 1、查询用户关系表，得到当前用户有哪些粉丝，遍历结果放在list集合中,如果没有粉丝，直接返回
            HTableInterface relationsTable = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS_TABLE_NAME));
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(Constant.WEIBO_RELATIONS_TABLE.COLUMN_FAMILY_FANS.getvalue()));
            Cell[] cells = relationsTable.get(get).rawCells();
            List<byte[]> fansList = new ArrayList<byte[]>(cells.length);

            for (Cell cell : cells) {
                fansList.add(CellUtil.cloneQualifier(cell));
            }

            if (fansList.size() <= 0) return;

            // 2、向微博收件箱表中批量添加rowkey
            HTableInterface receive_content_email = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME));
            List<Put> put4RCETableList = new ArrayList<Put>(fansList.size());

            for (byte[] rowBytes : fansList) {
                Put put4RCETable = new Put(rowBytes);
                put4RCETable.add(Bytes.toBytes(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE.COLUMN_FAMILY_INFO.getvalue()),
                        Bytes.toBytes(uid), timestamp, Bytes.toBytes(rowKeyOfContent));
                put4RCETableList.add(put4RCETable);
            }

            receive_content_email.put(put4RCETableList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != hConnection) {
                try {
                    hConnection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 关注用户逻辑
     * a、在微博用户关系表中，对当前主动操作的用户添加新的关注的好友
     * b、在微博用户关系表中，对被关注的用户添加粉丝（当前操作的用户）
     * c、当前操作用户的微博收件箱添加所关注的用户发布的微博rowkey
     */
    private void addAttends(String uid, String... attends) {
        int attendsLen = attends.length;

        if (StringUtils.isBlank(uid) || null == attends || attendsLen <= 0) {
            return;
        }

        HConnection hConnection = null;

        try {
            hConnection = HConnectionManager.createConnection(configuration);

            // a、为当前用户添加关注的人,为被关注的人，添加粉丝,将所有关注的人一个一个的添加到puts（List）集合中
            HTableInterface relationsTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS_TABLE_NAME));
            List<Put> relationsPutList = new ArrayList<Put>(attendsLen);
            Put attendPut = new Put(Bytes.toBytes(uid));

            for (String attend : attends) {
                attendPut.add(Bytes.toBytes(Constant.WEIBO_RELATIONS_TABLE.COLUMN_FAMILY_ATTENDS.getvalue()),
                        Bytes.toBytes(attend), Bytes.toBytes(attend));
                Put fansPut = new Put(Bytes.toBytes(attend));
                fansPut.add(Bytes.toBytes(Constant.WEIBO_RELATIONS_TABLE.COLUMN_FAMILY_FANS.getvalue()),
                        Bytes.toBytes(uid), Bytes.toBytes(uid));
                relationsPutList.add(fansPut);
            }

            relationsPutList.add(attendPut);
            relationsTBL.put(relationsPutList);

            // b.从内容表中取出所有的关注人的微博rowkey,表迭代的是一行数据，行再取得列！前置匹配过滤器
            HTableInterface contentTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT_TABLE_NAME));
            Scan contentScan = new Scan();
            List<byte[]> rowkeyList = new ArrayList<byte[]>(attendsLen);

            for (String attend : attends) {
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
                contentScan.setFilter(filter);
                Iterator<Result> iterator = contentTBL.getScanner(contentScan).iterator();

                while (iterator.hasNext()) {
                    //取出每一个符合扫描结果的那一行数据
                    Result result = iterator.next();

                    for (Cell cell : result.rawCells()) {
                        rowkeyList.add(CellUtil.cloneRow(cell));
                    }
                }
            }

            if (rowkeyList.size() <= 0) return;

            // c.将取出的微博rowkey放置于当前操作用户的收件箱中
            HTableInterface rceTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME));
            List<Put> rcePutList = new ArrayList<Put>(rowkeyList.size());

            for (byte[] rkByte : rowkeyList) {
                Put rcePut = new Put(Bytes.toBytes(uid));
                String rowkey4Content = Bytes.toString(rkByte);
                String attendUid = rowkey4Content.substring(0, rowkey4Content.indexOf("_"));
                long timestamp = Long.parseLong(rowkey4Content.substring(rowkey4Content.indexOf("_") + 1));
                rcePut.add(Bytes.toBytes(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE.COLUMN_FAMILY_INFO.getvalue()),
                        Bytes.toBytes(attendUid), timestamp, rkByte);
                rcePutList.add(rcePut);
            }

            rceTBL.put(rcePutList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != hConnection) {
                try {
                    hConnection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 取消关注（remove)
     * a、在微博用户关系表中，对当前主动操作的用户删除对应取关的好友
     * b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
     * c、从收件箱中，删除取关的人的微博的rowkey
     */
    private void removeAttends(String uid, String... attends) {
        int attendsLen = attends.length;

        if (StringUtils.isBlank(uid) || null == attends || attendsLen <= 0) {
            return;
        }

        HConnection hConnection = null;

        try {
            hConnection = HConnectionManager.createConnection(configuration);

            // a.用户关系表中，每次取消关注，被关注的用户粉丝 -1
            HTableInterface relationsTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS_TABLE_NAME));
            List<Delete> relationsDeleteList = new ArrayList<Delete>(attendsLen);
            Delete attendDelete = new Delete(Bytes.toBytes(uid));

            for (String attend : attends) {
                attendDelete.addColumn(Bytes.toBytes(Constant.WEIBO_RELATIONS_TABLE.COLUMN_FAMILY_ATTENDS.getvalue())
                        , Bytes.toBytes(attend));
                Delete fansDelete = new Delete(Bytes.toBytes(attend));
                fansDelete.addColumn(Bytes.toBytes(Constant.WEIBO_RELATIONS_TABLE.COLUMN_FAMILY_FANS.getvalue())
                        , Bytes.toBytes(uid));
                relationsDeleteList.add(fansDelete);
            }

            relationsDeleteList.add(attendDelete);
            relationsTBL.delete(relationsDeleteList);

            // 从收件箱中删除被关注的人的微博RowKey
            HTableInterface rceTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME));
            Delete rceDelete = new Delete(Bytes.toBytes(uid));

            for (String attend : attends) {
                rceDelete.deleteColumn(Bytes.toBytes(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE.COLUMN_FAMILY_INFO.getvalue())
                        , Bytes.toBytes(attend));
            }

            rceTBL.delete(rceDelete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != hConnection) {
                try {
                    hConnection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 获取微博实际内容
     * a、从微博收件箱中获取所有关注的人的发布的微博的rowkey
     * b、根据得到的rowkey去微博内容表中得到数据
     * c、将得到的数据封装到Message对象中
     */
    private List<Message> getAttendsContent(String uid) {

        if (StringUtils.isBlank(uid)) {
            return null;
        }

        HConnection hConnection = null;

        try {
            hConnection = HConnectionManager.createConnection(configuration);

            HTableInterface rceTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME));
            List<byte[]> rowKeyByteList = new ArrayList<byte[]>();
            Get rceGet = new Get(Bytes.toBytes(uid));
            rceGet.setMaxVersions(5);
            Result result = rceTBL.get(rceGet);

            for (Cell cell : result.rawCells()) {
                rowKeyByteList.add(CellUtil.cloneValue(cell));
            }

            HTableInterface contentTBL = hConnection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT_TABLE_NAME));
            List<Get> contentGetList = new ArrayList<Get>(rowKeyByteList.size());

            for (byte[] rkByte : rowKeyByteList) {
                Get get = new Get(rkByte);
                contentGetList.add(get);
            }

            Result[] results = contentTBL.get(contentGetList);
            List<Message> messageList = new ArrayList<Message>();

            for (Result res : results) {
                for (Cell cell : res.rawCells()) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String userid = rowKey.substring(0, rowKey.indexOf("_"));
                    String timestamp = rowKey.substring(rowKey.indexOf("_") + 1);
                    String content = Bytes.toString(CellUtil.cloneValue(cell));

                    Message message = new Message(userid, timestamp, content);
                    messageList.add(message);
                }
            }

            return messageList;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    /**
     * 初始化名称空间，命名空间类似于关系型数据库中的schema，可以想象成文件夹
     */
    private void initNameSpace() {
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor
                    .create(Constant.WEIBO_NAME_SPACE)
                    .addConfiguration("creator", "lanpeng")
                    .addConfiguration("create_time", "2019-3-7 16：44")
                    .build();
            hBaseAdmin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeAdmin(hBaseAdmin);
        }
    }

    /**
     * 初始化微博内容表
     * Table Name:weibo:content
     * RowKey:用户ID_时间戳
     * ColumnFamily:info
     * ColumnLabel:标题	内容		图片URL
     * Version:1个版本
     */
    private void creatTableContent() {
        HBaseAdmin hBaseAdmin = null;

        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            // 表描述对象，列族描述对象，块缓存，块缓存大小，压缩方式，版本确界
            HTableDescriptor content = new HTableDescriptor(TableName.valueOf(Constant.WEIBO_CONTENT_TABLE_NAME));
            HColumnDescriptor infoCF = new HColumnDescriptor(Bytes.toBytes(Constant.WEIBO_CONTENT_TABLE.COLUMN_FAMILY_INFO.getvalue()));
            infoCF.setBlockCacheEnabled(true);
            infoCF.setBlocksize(2097152);
//            infoCF.setCompressionType(Compression.Algorithm.SNAPPY);
            infoCF.setMaxVersions(1);
            infoCF.setMinVersions(1);
            content.addFamily(infoCF);

            hBaseAdmin.createTable(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeAdmin(hBaseAdmin);
        }
    }

    /**
     * 用户关系表
     * Table Name:weibo:relations
     * RowKey:用户ID
     * ColumnFamily:attends,fans
     * ColumnLabel:关注用户ID，粉丝用户ID
     * ColumnValue:用户ID
     * Version：1个版本
     */
    private void createTableRelations() {
        HBaseAdmin hBaseAdmin = null;

        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            HTableDescriptor relations = new HTableDescriptor(TableName.valueOf(Constant.WEIBO_RELATIONS_TABLE_NAME));
            Constant.WEIBO_RELATIONS_TABLE[] valuesOfCF = Constant.WEIBO_RELATIONS_TABLE.values();

            for (Constant.WEIBO_RELATIONS_TABLE CFS : valuesOfCF) {
                HColumnDescriptor cf = new HColumnDescriptor(Bytes.toBytes(CFS.getvalue()));
                cf.setBlockCacheEnabled(true);
                cf.setBlocksize(2097152);
                cf.setMinVersions(1);
                cf.setMaxVersions(1);

                relations.addFamily(cf);
            }

            hBaseAdmin.createTable(relations);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeAdmin(hBaseAdmin);
        }
    }

    /**
     * 创建微博收件箱表
     * Table Name: weibo:receive_content_email
     * RowKey:用户ID
     * ColumnFamily:info
     * ColumnLabel:用户ID-发布微博的人的用户ID
     * ColumnValue:关注的人的微博的RowKey
     * Version:1000
     */
    private void createTableReceiveContentEmails() {
        HBaseAdmin hBaseAdmin = null;

        try {
            hBaseAdmin = new HBaseAdmin(configuration);
            HTableDescriptor receive_content_email = new HTableDescriptor(
                    TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE_NAME));
            HColumnDescriptor info = new HColumnDescriptor(Constant.WEIBO_RECEIVE_CONTENT_EMAIL_TABLE.COLUMN_FAMILY_INFO.getvalue());
            info.setBlockCacheEnabled(true);
            info.setBlocksize(2097152);
            info.setMaxVersions(1000);
            info.setMinVersions(1000);
            receive_content_email.addFamily(info);

            hBaseAdmin.createTable(receive_content_email);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeAdmin(hBaseAdmin);
        }

    }

    /**
     * 关闭admin对象
     */
    private void closeAdmin(HBaseAdmin hBaseAdmin) {
        if (null != hBaseAdmin) {
            try {
                hBaseAdmin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        WeiBo weibo = new WeiBo();
//        weiBo.initWEIBOHbase();
        weibo.testPublishContent(weibo);
        weibo.testAddAttend(weibo);
        weibo.testShowMessage(weibo);
        weibo.testRemoveAttend(weibo);
        weibo.testShowMessage(weibo);

        System.out.println("done");
    }
}
