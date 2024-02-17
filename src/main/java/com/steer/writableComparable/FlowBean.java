package com.steer.writableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现WritableComparable接口
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private Long upFlow;
    private Long downFlow;

    private Long sumNum;

    public FlowBean(){};
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumNum = dataInput.readLong();
    }

    public Long getSumNum() {
        return sumNum;
    }

    public void setSumNum(Long sumNum) {
        this.sumNum = sumNum;
    }

    public void caculateNum(){
        this.sumNum = this.upFlow+this.downFlow;
    }


    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return  upFlow + " "  + downFlow +" "+ sumNum;
    }

    @Override
    public int compareTo(FlowBean o) {
        //倒序
        if (this.sumNum < o.getSumNum()){
            return 1;
        }else if(this.sumNum > o.getSumNum()){
            return -1;
        }else {
            //总流量相同的情况，排上行流量，正序
            if (this.upFlow < o.getUpFlow()){
                return -1;
            }else if(this.upFlow > o.getUpFlow()){
                return 1;
            }
        }
        return 0;
    }
}
