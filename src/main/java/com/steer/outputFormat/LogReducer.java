package com.steer.outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //防止有相同数据，丢数据.eg. http://www.baidu.com ,http://www.baidu.com 多条的情况
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
