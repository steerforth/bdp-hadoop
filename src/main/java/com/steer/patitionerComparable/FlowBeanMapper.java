package com.steer.patitionerComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 1.map阶段输入key,value
 * LongWritable,Text
 * 2.map阶段输出key,value
 * Text,IntWritable
 */
public class FlowBeanMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowBeanMapper.class);

    private Text outV = new Text();
    private FlowBean outKey = new FlowBean();

    /**
     *
     * @param key 偏移量
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        LOGGER.info("map: key:{},value:{}",key.get(),value.toString());
        String line = value.toString();
        String[] words = line.split(" ");

//        String num = words[0];
        String phoneNum = words[1];
        String upFlow = words[2];
        String downFlow = words[3];

        outV.set(phoneNum);
        outKey.setUpFlow(Long.valueOf(upFlow));
        outKey.setDownFlow(Long.valueOf(downFlow));
        outKey.caculateNum();
        //按key进行排序
        context.write(outKey,outV);


    }
}
