package com.mnw.hbase;

import com.mnw.hbase.info.TableInfo;
import com.mnw.hbase.mapper.RmeThirdMapper;
import com.mnw.hbase.mapper.ThirdFileMapper;
import com.mnw.hbase.reduce.RmeThirdReduce;
import com.mnw.hbase.reduce.ThirdFileReduce;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by shaodi.chen on 2018/11/2.
 */
public class RmeThirdMr {

    private static final Log LOGGER = LogFactory.getLog(RmeThirdMr.class.getName());
    private final static String NAME = "二级索引";

    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        conf.set("hbase.master", "hadoop1:60000");
        conf.set("mapreduce.output.fileoutputformat.compress", "false");
        //conf.set("mapreduce.map.memory.mb", "2048");
        //conf.set("mapreduce.map.java.opts", "-Xmx1024m");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        String[] tableInfo = args[0].split(TableInfo.SPLITTER1, -1);

        String databaseName = tableInfo[0];
        String foreignTable = tableInfo[1];
        String tableName = tableInfo[2];
        String foreignKey = tableInfo[3];
        String rowKey = tableInfo[4];
        String row2Column = tableInfo[5];

        String tableFilePrefix = "/user/hive/warehouse/";
        String tmpPath = "/tmp/hbaseMr";
        String cashPath = "/cashFile/";

        conf.set("cashPath",cashPath);

        conf.set("databaseName", databaseName);
        conf.set("foreignTable", foreignTable);
        conf.set("tableName", tableName);
        conf.set("foreignKey", foreignKey);
        conf.set("rowKey", rowKey);
        conf.set("row2Column", row2Column);
        conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");

        LOGGER.info("conf set over");

        Job job = Job.getInstance(conf, NAME);

        job.setJarByClass(RmeThirdMr.class);

        Scan scan = new Scan();

        scan.setCaching(5000);

        scan.setCacheBlocks(false);

        LOGGER.info("scan set over,start MapReduce");

        TableMapReduceUtil.initTableMapperJob(databaseName, scan, RmeThirdMapper.class, Text.class, Text.class, job);
        job.setReducerClass(RmeThirdReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(tmpPath));

//        TableMapReduceUtil.initTableReducerJob("rme", RmeThirdReduce.class,job);


        Job job1 = Job.getInstance(conf, "deal data");

        job1.setJarByClass(RmeThirdMr.class);

        job1.setMapperClass(ThirdFileMapper.class);
        TableMapReduceUtil.initTableReducerJob(databaseName, ThirdFileReduce.class, job1);

        URI cashFile = new URI("hdfs://192.168.5.60:9000"+cashPath+"/"+tableName);
        job1.addCacheFile(cashFile);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job1, tableFilePrefix + databaseName + ".db/" + tableName + "/");
        FileInputFormat.addInputPaths(job1, tmpPath);

        FileSystem dfs = FileSystem.get(conf);
        Path outPath1 = new Path(tmpPath);

        if (dfs.exists(outPath1)) {
            dfs.delete(outPath1, true);
        }

        boolean isSucess = job.waitForCompletion(true);

        if (job.waitForCompletion(true)) {
            LOGGER.info("Job1 over");
            isSucess = job1.waitForCompletion(true);
        }
        LOGGER.info("All job over");
        System.out.println(isSucess);

    }
}
