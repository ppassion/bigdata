package com.cyh.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducerForWritable extends Reducer<Text, MyFlowBean,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<com.cyh.writable.MyFlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCount = 0;
        int downCount = 0;
        for (MyFlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCount += value.getUpCount();
            downCount += value.getDownFlow();
        }
        context.write(key,new Text(upFlow + "," + downFlow + "," + upCount + "," + downCount));
    }
}
