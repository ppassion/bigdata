package com.cyh.mapreduce.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        Text text = new Text();
        for (String word : split) {
            text.set(word);
            context.write(text,new IntWritable(1));
        }
    }
}
