package com.steer.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlowBeanDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowBeanDriver.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(FlowBeanDriver.class);
        //job.setJar("E:\\workspace\\bdp-mr\\bdp-mr\\target\\bdp-mr-1.0-SNAPSHOT.jar");

        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        //设置map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置最终输出key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //**
        job.setPartitionerClass(MyPartitioner.class);
        //设置为1，走默认的Partition；设置大于getPartion的数量，会多空文件;其他则报错
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadoopTest\\input_phone"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopTest\\output_phone"));

        boolean b = job.waitForCompletion(true);
        LOGGER.info("执行结果:"+b);
        System.exit(b? 0:1 );
    }
}
