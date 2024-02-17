package com.steer.wordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountCombiner.class);

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
