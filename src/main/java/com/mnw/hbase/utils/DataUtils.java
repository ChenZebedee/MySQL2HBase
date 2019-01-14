package com.mnw.hbase.utils;

import com.mnw.hbase.info.TableInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shaodi.chen on 2018/11/21.
 */

@Getter
@Setter
public class DataUtils {

    /**
     * 将一条数据变成一列
     *
     * @param listData
     * @param row2Column
     * @return
     */
    public static Map<String,String> getColumnData(List<Map<String, String>> listData, String row2Column) {

        String[] columns = StringUtils.split(row2Column, ",");

        Map<String,String> outMap = new HashedMap();

        for (Map<String,String> mapData:listData) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < columns.length - 1; i++) {
                sb.append(mapData.get(columns[i]));
                if (i != columns.length - 2) {
                    sb.append("_");
                }
            }
            outMap.put(sb.toString(),mapData.get(columns[columns.length-1]));
        }
        return outMap;
    }


    /**
     * 将 MySQL 获取的数据放入到 List 中
     *
     * @param dataList
     * @param columnName
     * @return
     */
    public static List<Map<String, String>> listData(List<String> dataList,List<String> columnName) {
        Map<String, String> dataMap = new HashMap<>();
        List<Map<String,String>> outList = new ArrayList<>();

        for (String dataLine:dataList) {
            String[] datas = dataLine.split(TableInfo.TABLE_SPLITTER,-1);
            dataMap.clear();
            for (int i = 0; i < datas.length; i++) {
                dataMap.put(columnName.get(i), datas[i]);
            }
            outList.add(dataMap);
        }

        return outList;
    }


    public static List<String> getTableInfo(Configuration configuration){
        DbManger dbManger = new DbManger();
        dbManger.getConn();
        return dbManger.getTableInfo(configuration.get("databaseName"),configuration.get("tableName"));
    }


}
