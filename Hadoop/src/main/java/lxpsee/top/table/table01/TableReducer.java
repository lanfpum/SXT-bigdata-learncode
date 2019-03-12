package lxpsee.top.table.table01;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 10:46.
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        try {
            TableBean productBean = new TableBean();
            List<TableBean> tableBeans = new ArrayList<TableBean>();

            for (TableBean bean : values) {
                String flag = bean.getFlag();

                if ("0".equals(flag)) {
                    TableBean order = new TableBean();
                    BeanUtils.copyProperties(order, bean);
                    tableBeans.add(order);
                } else if ("1".equals(flag)) {
                    BeanUtils.copyProperties(productBean, bean);
                }
            }

            for (TableBean tableBean : tableBeans) {
                tableBean.setProductName(productBean.getProductName());
                context.write(tableBean, NullWritable.get());
            }

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
