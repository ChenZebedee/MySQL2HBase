package com.mnw.hbase.reduce;

import com.mnw.hbase.info.TableInfo;
import com.mnw.hbase.utils.DataUtils;
import com.mnw.hbase.utils.DbManger;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by shaodi.chen on 2018/12/3.
 */
public class ThirdFileReduce extends TableReducer<Text, Text, NullWritable> {

    private Text outKey = new Text();
    private Text outValue = new Text();


    List<String> tableInfo = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] URI = context.getCacheFiles();
        FileSystem hdfs = FileSystem.get(context.getConfiguration());

        String line = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(URI[0].getPath())), StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                tableInfo.add(line.split("\t",-1)[0]);
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String row2Column = configuration.get("row2Column");

        List<String> thirdData = new ArrayList<>();
        for (Text value:values){
            String[] data = StringUtils.split(value.toString(), TableInfo.TABLE_SPLITTER,-1);
            if (data.length==1){
                outKey.set(data[0]);
            }else if (data.length==tableInfo.size()){
                thirdData.add(value.toString());
            }
        }
        Put put = new Put(outKey.getBytes());
        if (thirdData.size()==0){
            return;
        }
        List<Map<String,String>> dataListMap = DataUtils.listData(thirdData,tableInfo);
        Map<String,String> dealData = new HashMap<>();
        if (!StringUtils.equals("",row2Column)){
            dealData.putAll(DataUtils.getColumnData(dataListMap,row2Column));
        }else{
            dealData.putAll(dataListMap.get(0));
        }
        for (String columnName:dealData.keySet()){
            put.addColumn(configuration.get("tableName").getBytes(),columnName.getBytes(),dealData.get(columnName).getBytes());
        }
        context.write(NullWritable.get(),put);
    }
}
