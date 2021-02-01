package com.cyh.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReducerJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path("D:\\tmp\\20210126"))) {
            fileSystem.delete(new Path("D:\\tmp\\20210126"),true);
        }

        Job job = Job.getInstance(super.getConf(), "reducerJoin");
        job.setJarByClass(ReducerJoinMain.class);


        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D:\\tmp\\input\\reduce-join"));


        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //将我们的文件输出成为sequenceFile这种格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D:\\tmp\\20210126"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ReducerJoinMain(), args);
        System.exit(run);
    }
}
