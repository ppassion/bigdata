package com.cyh.mapreduce.compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapperForCompare extends Mapper<LongWritable, Text,Text, MyFlowBean> {
    private MyFlowBean flowBean;
    private Text phoneNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new MyFlowBean();
        phoneNumber = new Text();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phone = fields[1];
        int upFlow = Integer.parseInt(fields[6]);
        int downFlow = Integer.parseInt(fields[7]);
        int upCount = Integer.parseInt(fields[8]);
        int downCount = Integer.parseInt(fields[9]);

        phoneNumber.set(phone);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCount(upCount);
        flowBean.setDownCount(downCount);
        context.write(phoneNumber,flowBean);
    }
}
