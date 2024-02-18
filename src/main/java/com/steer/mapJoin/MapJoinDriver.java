package com.steer.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 完善ReduceJoin的代码
 * 将Reduce的工作提前到Map阶段执行
 *
 * 按不了id排序？
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(MapJoinDriver.class);
        //job.setJar("E:\\workspace\\bdp-mr\\bdp-mr\\target\\bdp-mr-1.0-SNAPSHOT.jar");

        job.setMapperClass(MapJoinMapper.class);
        //job.setReducerClass(TableReducer.class);
        //设置map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //集群为hdfs://
        job.addCacheFile(new URI("file:///E:/hadoopTest/input_table/pd.txt"));
        //不需要reduce阶段
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("E:\\hadoopTest\\input_mapjoin"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopTest\\output_mapjoin"));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1 );
    }
}
