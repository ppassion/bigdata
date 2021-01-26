package com.cyh.writable;

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


public class MainForWritable extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, MainForWritable.class.getSimpleName());
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path("file:///D:\\tmp\\20210126"))) {
            fileSystem.delete(new Path("file:///D:\\tmp\\20210126"),true);
        }
        job.setJarByClass(MainForWritable.class);

        //1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\tmp\\input"));

        //2
        job.setMapperClass(MyMapperForWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFlowBean.class);

        //7
        job.setReducerClass(MyReducerForWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //8
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\tmp\\20210126"));

        //job.setNumReduceTasks(4);
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //configuration.set("mapreduce.framework.name","local");
        //onfiguration.set("yarn.resourcemanager.hostname","local");
        int run = ToolRunner.run(configuration,new MainForWritable(),args);
        System.exit(run);
    }
}
