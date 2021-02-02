package com.cyh.mapreduce.compare;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class MainForCompare extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Logger.getLogger(MainForCompare.class).setLevel(Level.ALL);
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, MainForCompare.class.getSimpleName());
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path("file:///D:\\tmp\\20210202"))) {
            fileSystem.delete(new Path("file:///D:\\tmp\\20210202"),true);
        }
        job.setJarByClass(MainForCompare.class);


        //1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\tmp\\input\\data_flow.dat"));

        //2
        job.setMapperClass(MyMapperForCompare.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFlowBean.class);

        //7
        job.setReducerClass(MyReducerForCompare.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //8
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\tmp\\20210202"));

        //job.setNumReduceTasks(4);
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name","local");
        configuration.set("yarn.resourcemanager.hostname","local");
        int run = ToolRunner.run(configuration,new MainForCompare(),args);
        System.exit(run);
    }
}
