package com.mnw.hbase.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shaodi.chen on 2018/11/9.
 */
public class RmeThirdReduce extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(key, value);
        }

    }
}
