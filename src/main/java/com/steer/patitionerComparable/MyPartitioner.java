package com.steer.patitionerComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * key,value对应的是Mapper的
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone =text.toString();
        if (phone.startsWith("137")){
            return 1;
        }else if (phone.startsWith("187")){
            return 2;
        }
        return 0;
    }

}
