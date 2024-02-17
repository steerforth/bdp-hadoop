package com.steer.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream baiduOut;
    private FSDataOutputStream oherOut;
    public LogRecordWriter(TaskAttemptContext context) {
        try {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            baiduOut = fileSystem.create(new Path("E:\\hadoopTest\\output_outformat\\baidu.log"));
            oherOut = fileSystem.create(new Path("E:\\hadoopTest\\output_outformat\\other.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String key = text.toString();
        //写入不同的文件中去
        if (key.contains("baidu.com")){
            baiduOut.writeBytes(key+"\n");
        }else {
            oherOut.writeBytes(key+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(baiduOut);
        IOUtils.closeStream(oherOut);
    }
}
