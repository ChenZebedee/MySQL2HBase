package com.mnw.hbase.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by shaodi.chen on 2018/11/2.
 */
public class RmeThirdMapper extends TableMapper<Text, Text> {


    private Text newKey = new Text();
    private Text newValue = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Configuration conf = HBaseConfiguration.create(context.getConfiguration());

        byte[] rowKey = value.getRow();
        String strRowKey = new String(rowKey,"UTF-8");

        newValue.set(strRowKey);

        byte[] outkey = value.getValue(conf.get("foreignTable").getBytes(), conf.get("foreignKey").getBytes());

        String strOutKey = new String(outkey, "UTF-8");

        newKey.set(strOutKey+"\t"+strRowKey);

        context.write(newKey, newValue);

    }

}
