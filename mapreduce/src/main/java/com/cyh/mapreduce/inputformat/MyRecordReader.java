package com.cyh.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private boolean isread = false;
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isread) {
            long length = fileSplit.getLength();
            byte[] bytes = new byte[(int) length];
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);

            IOUtils.readFully(inputStream,bytes,0, (int) length);

            bytesWritable.set(bytes,0, (int) length);

            isread = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isread ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
