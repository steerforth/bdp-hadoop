package com.steer.combineTextFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 1.map阶段输入key,value
 * LongWritable,Text
 * 2.map阶段输出key,value
 * Text,IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountMapper.class);

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
//        LOGGER.info("map: key:{},value:{}",key.get(),value.toString());
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word :
                words) {
            text.set(word);
            context.write(text,val);
        }


    }
}
