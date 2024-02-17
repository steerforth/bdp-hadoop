package com.steer.reduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        List<TableBean> orderList = new ArrayList<>();
        TableBean pdBean = new TableBean();//商品只有一个

        for (TableBean value : values) {
            if ("order".equals(value.getType())){
                TableBean tmBean = new TableBean();
                tmBean.setId(value.getId());
                tmBean.setPid(value.getPid());
                tmBean.setAmount(value.getAmount());
                tmBean.setPname(value.getPname());
                tmBean.setType(value.getType());
                orderList.add(tmBean);
//                orderList.add(value);  //不正确
            }else{
                pdBean.setId(value.getId());
                pdBean.setPid(value.getPid());
                pdBean.setAmount(value.getAmount());
                pdBean.setPname(value.getPname());
                pdBean.setType(value.getType());
            }
        }

        for (TableBean bean : orderList) {
            bean.setPname(pdBean.getPname());
            context.write(bean,NullWritable.get());
        }


    }
}
