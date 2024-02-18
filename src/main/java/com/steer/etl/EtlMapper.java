package com.steer.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private static final Pattern pattern = Pattern.compile("\\d+");
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (!match(line)){
            return;
        }

        context.write(value,NullWritable.get());
    }

    private boolean match(String line) {
        Matcher matcher = pattern.matcher(line);
        return matcher.matches();
    }
}
