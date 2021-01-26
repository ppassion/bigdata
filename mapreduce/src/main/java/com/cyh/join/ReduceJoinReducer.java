package com.cyh.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text,Text,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> orders = new ArrayList<>();
        String product = "";

        for (Text text : values) {
            if ("p".equals(text.toString().charAt(0))) {
                product = text.toString();
            } else {
                orders.add(text.toString());
            }
        }

        for (String order : orders) {
            context.write(new Text(order + "\t" + product), NullWritable.get());
        }
    }
}
