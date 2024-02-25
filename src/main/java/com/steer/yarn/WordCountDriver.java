package com.steer.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * yarn jar bdp-mr.jar com.steer.yarn.WordCountDriver wordcount E:\\hadoopTest\\input E:\\hadoopTest\\output1
 */
public class WordCountDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountDriver.class);
    private static Tool tool;
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        switch (args[0]){
            case "wordcount":
                tool = new WorkCount();
                break;
            default:
                throw new RuntimeException("no such tool:"+args[0]);
        }


       int run= ToolRunner.run(configuration,tool, Arrays.copyOfRange(args,1,args.length));
        System.exit(run);
    }
}
