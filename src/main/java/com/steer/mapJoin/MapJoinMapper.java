package com.steer.mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Map<String,String> pdMap = new HashMap<>();
    private Text outK = new Text();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream stream = fileSystem.open(new Path(cacheFiles[0]));

        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        String line;
        while ((line = reader.readLine())!= null){
            String[] split = line.split(" ");
            //key为Pid，value为pname
            pdMap.put(split[0],split[1]);
        }

        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");

        String pname = pdMap.get(split[1]);
        //订单id,名称，数量
        outK.set(split[0]+" "+pname+" "+split[2]);
        context.write(outK,NullWritable.get());

    }
}
