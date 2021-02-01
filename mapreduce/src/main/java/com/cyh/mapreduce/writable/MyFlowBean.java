package com.cyh.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyFlowBean implements Writable {

    private Integer upFlow;
    private Integer downFlow;
    private Integer upCount;
    private Integer downCount;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(upCount);
        out.writeInt(downFlow);
        out.writeInt(downCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        upCount = in.readInt();
        downFlow = in.readInt();
        downCount = in.readInt();
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCount() {
        return upCount;
    }

    public void setUpCount(Integer upCount) {
        this.upCount = upCount;
    }

    public Integer getDownCount() {
        return downCount;
    }

    public void setDownCount(Integer downCount) {
        this.downCount = downCount;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", upCount=" + upCount +
                ", downCount=" + downCount +
                '}';
    }
}
