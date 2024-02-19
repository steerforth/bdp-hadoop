package com.steer.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCountDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountDriver.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置map输出端的压缩
        conf.setBoolean("mapreduce.map.output.compress",true);
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        //设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        //job.setJar("E:\\workspace\\bdp-mr\\bdp-mr\\target\\bdp-mr-1.0-SNAPSHOT.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadoopTest\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopTest\\output"));

        //设置最终端的压缩
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        boolean b = job.waitForCompletion(true);
        LOGGER.info("执行结果:"+b);
        System.exit(b? 0:1 );
    }
}
