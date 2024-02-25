package com.steer.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 1.reduce阶段输入key,value
 * Text,IntWritable
 * 2.reduce阶段输入key,value
 * Text,IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountReducer.class);

    private IntWritable val = new IntWritable();

    @Override
    protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        LOGGER.info("reduce: key:{}",key.toString());
        int sum = 0;
        for (IntWritable intWritable :values){
            LOGGER.info("reduce: value:{}",intWritable.get());
            sum += intWritable.get();

        }
        val.set(sum);
        context.write(key,val);
    }



}
