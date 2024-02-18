package com.steer.reduceJoin;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements WritableComparable<TableBean> {
    private String id;//订单id
    private String pid;//商品id
    private int amount;//商品数量
    private String pname;//商品名称
    private String type;// order/pd

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.type = dataInput.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return id+" "+pname+" "+amount;
    }

    @Override
    public int compareTo(TableBean o) {
        if( Integer.parseInt(this.getId()) < Integer.parseInt(o.getId())){
            return 1;
        }else if (Integer.parseInt(this.getId()) > Integer.parseInt(o.getId())){
            return -1;
        }
        return 0;
    }
}
