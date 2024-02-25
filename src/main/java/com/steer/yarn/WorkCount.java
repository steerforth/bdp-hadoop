package com.steer.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class WorkCount implements Tool {

    private Configuration configuration;
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
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

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b? 0:1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        private Text text = new Text();
        private IntWritable val = new IntWritable(1);

        /**
         *
         * @param key 偏移量
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word :
                    words) {
                text.set(word);
                context.write(text,val);
            }


        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        private IntWritable val = new IntWritable();

        @Override
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable intWritable :values){
                sum += intWritable.get();
            }
            val.set(sum);
            context.write(key,val);
        }
    }
}
