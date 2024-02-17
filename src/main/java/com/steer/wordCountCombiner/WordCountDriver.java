package com.steer.wordCountCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

        //**实际也可以用WordCountReducer
        job.setCombinerClass(WordCountCombiner.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadoopTest\\input_word"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopTest\\output_word"));

        boolean b = job.waitForCompletion(true);
        LOGGER.info("执行结果:"+b);
        System.exit(b? 0:1 );
    }
}
