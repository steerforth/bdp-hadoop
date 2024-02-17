package com.steer.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 1.reduce阶段输入key,value
 * Text,IntWritable
 * 2.reduce阶段输入key,value
 * Text,IntWritable
 */
public class FlowBeanReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowBeanReducer.class);

    @Override
    protected void reduce(FlowBean key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
//        LOGGER.info("reduce: key:{}",key.toString());
        //可能存在手机号不同，总流量重复的情况
        //最后仍然期望输出手机号在前，流量在后的格式
        for (Text val :values){
//            LOGGER.info("reduce: value:{}",bean.toString());
            context.write(val,key);
        }
    }

}
