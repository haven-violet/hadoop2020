package com.qf.mr.phone;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义手机流量实体类
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public void set(Long upFlow, Long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowBean flowBean = (FlowBean) o;

        if (upFlow != null ? !upFlow.equals(flowBean.upFlow) : flowBean.upFlow != null) return false;
        if (downFlow != null ? !downFlow.equals(flowBean.downFlow) : flowBean.downFlow != null) return false;
        return sumFlow != null ? sumFlow.equals(flowBean.sumFlow) : flowBean.sumFlow == null;
    }

    @Override
    public int hashCode() {
        int result = upFlow != null ? upFlow.hashCode() : 0;
        result = 31 * result + (downFlow != null ? downFlow.hashCode() : 0);
        result = 31 * result + (sumFlow != null ? sumFlow.hashCode() : 0);
        return result;
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

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        int i = this.sumFlow.compareTo(o.getSumFlow());
        return -i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}
