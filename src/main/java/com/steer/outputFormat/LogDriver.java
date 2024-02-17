package com.steer.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(LogDriver.class);
//        job.setJar("E:\\workspace\\bdp-mr\\bdp-mr\\target\\bdp-mr-1.0-SNAPSHOT.jar");

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        //设置map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //***
        job.setOutputFormatClass(LogOutputformat.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadoopTest\\input_outformat"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopTest\\output_outformat"));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1 );
    }
}
