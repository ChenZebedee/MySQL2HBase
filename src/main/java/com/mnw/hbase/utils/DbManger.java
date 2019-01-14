package com.mnw.hbase.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shaodi.chen on 2018/11/27.
 */
public class DbManger {
    static final String JDBC = "com.mysql.cj.jdbc.Driver";
    static final String URL = "jdbc:mysql://192.168.1.83:3306/rme";
    static final String USER = "admin";
    static final String PASSWD = "Mnw!@#456";
    static Connection connection;
    static Statement statement;

    public void getConn(){
        try {
            Class.forName(JDBC);
            connection = DriverManager.getConnection(URL, USER, PASSWD);
            statement = connection.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet getData(String tableName, String rowKey, String id) {
        ResultSet resultSet = null;
        String sqlTxt = "select * from " + tableName + " where " + rowKey + "='" + id + "'";
        System.out.println("sqlTxt: " + sqlTxt);
        try {
            resultSet = statement.executeQuery(sqlTxt);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    public List<String> getTableInfo(String databaseName, String tableName){
        List<String> tableInfoList = new ArrayList<>();
        String sqlTxt = "select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA='"+databaseName+"' and TABLE_NAME='"+tableName+"' order by ORDINAL_POSITION asc " ;

        try {
            ResultSet resultSet = statement.executeQuery(sqlTxt);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tableInfoList;
    }

    public void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
