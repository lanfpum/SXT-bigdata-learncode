package lxpsee.top.hbase.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/20 17:25.
 * <p>
 * configuration    HBase和Hadoop的管理配置对象
 */
public class CRUDBaseHBase {
    private static Configuration configuration;

    static {
        configuration = HBaseConfiguration.create();
    }

    /**
     * 判断表是否已经存在，在hbase中，管理、访问表都是需要首先创建HBaseAdmin对象
     *
     * @param tableName
     * @return
     */
    private static boolean isTableExist(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            return admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 创建表
     * 1.判断表是否存在
     * 2.创建表属性描述对象，表名需要转字节
     * 3.创建多个列族
     * 4.根据对表的配置，创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     */
    private static void createTable(String tableName, String... columnFamily) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);

            if (isTableExist(tableName)) {
                System.out.println("表" + tableName + "已经存在！");
                System.exit(0);
            }

            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    private static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);

            if (isTableExist(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("表" + tableName + "删除成功");
            } else {
                System.out.println("表" + tableName + "不存在！！！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 往表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     */
    private static void putData2Table(String tableName, String rowKey, String columnFamily, String column, String value) {
        try {
            HTable table = new HTable(configuration, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
            System.out.println("表" + tableName + "插入数据成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量删除表中数据
     *
     * @param tableName
     * @param rows
     */
    private static void deleteDataMultiRow(String tableName, String... rows) {
        try {
            HTable table = new HTable(configuration, tableName);
            List<Delete> deleteList = new ArrayList<Delete>(rows.length);

            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }

            table.delete(deleteList);
            table.close();
            System.out.println("表" + tableName + "删除" + rows + "数据成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getAllRowData(String tableName) {
        try {
            HTable table = new HTable(configuration, tableName);
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);

            for (Result result : results) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    StringBuilder show = new StringBuilder();
                    show.append(Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
                    show.append(Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                    show.append(Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                    show.append(Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println(show);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        System.out.println(isTableExist("lanpeng:students"));
//        createTable("lanpeng:person", "dept", "base_info");
//        dropTable("lanpeng:person");
        /*putData2Table("lanpeng:person", "personRowKey001", "dept", "sale", "ballsaller");
        putData2Table("lanpeng:person", "personRowKey001", "dept", "code", "javaCoder");
        putData2Table("lanpeng:person", "personRowKey001", "dept", "code", "bigdata");
        putData2Table("lanpeng:person", "personRowKey001", "base_info", "name", "jim");
        putData2Table("lanpeng:person", "personRowKey001", "base_info", "age", "25");
*/

//        deleteDataMultiRow("lanpeng:person", "personRowKey001");
        getAllRowData( "lanpeng:students");

    }
}
