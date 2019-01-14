package com.mnw.hbase.mapper;

import com.mnw.hbase.info.TableInfo;
import com.mnw.hbase.utils.DataUtils;
import com.mnw.hbase.utils.DbManger;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shaodi.chen on 2018/12/3.
 */
public class ThirdFileMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> tableInfo = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //预处理，把要关联的文件加载到缓存中
        URI[] URI = context.getCacheFiles();
        //新的检索缓存文件的API是 context.getCacheFiles() ，而 context.getLocalCacheFiles() 被弃用
        //然而 context.getCacheFiles() 返回的是 HDFS 路径； context.getLocalCacheFiles() 返回的才是本地路径
        FileSystem hdfs = FileSystem.get(context.getConfiguration());

        //这里只缓存了一个文件，所以取第一个即可
        String line = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(URI[0].getPath())), StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                tableInfo.add(line.split("\t",-1)[0]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Configuration configuration = context.getConfiguration();
        String line = value.toString();
        String[] values = line.split(TableInfo.TABLE_SPLITTER,-1);
        String foreignKey = configuration.get("foreignKey");
        if (values.length==2){
            outKey.set(values[0]);
            outValue.set(values[1]);
        }else if (values.length==tableInfo.size()){
            outKey.set(values[tableInfo.indexOf(foreignKey)]);
            outValue.set(line);
        }
        context.write(outKey,outValue);
    }
}
