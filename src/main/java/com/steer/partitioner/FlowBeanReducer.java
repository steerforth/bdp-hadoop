package com.steer.partitioner;

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
public class FlowBeanReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowBeanReducer.class);

    private FlowBean val = new FlowBean();

    @Override
    protected void reduce(Text key,Iterable<FlowBean> values,Context context) throws IOException, InterruptedException {
//        LOGGER.info("reduce: key:{}",key.toString());
        long upFlow = 0;
        long downFlow = 0;
        for (FlowBean bean :values){
//            LOGGER.info("reduce: value:{}",bean.toString());
            upFlow += bean.getUpFlow();
            downFlow += bean.getDownFlow();
        }

        val.setUpFlow(upFlow);
        val.setDownFlow(downFlow);
        val.caculateNum();
        context.write(key,val);
    }

}
