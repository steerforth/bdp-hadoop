package com.steer.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * key,value对应的是Mapper的
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone =text.toString();
        if (phone.startsWith("137")){
            return 1;
        }else if (phone.startsWith("187")){
            return 2;
        }
        return 0;
    }
}
