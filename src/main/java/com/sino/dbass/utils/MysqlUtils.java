package com.sino.dbass.utils;

import com.sino.dbass.pojo.MysqlTwoPojo;
import com.sino.dbass.pojo.OracleLogPojoFromMysql;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlUtils {
    private static Logger logger = Logger.getLogger(MysqlUtils.class);

    public static Connection getConnection(String ip,String schema,String username,String password){
        String url = "jdbc:mysql://" + ip + "/" + schema + "?characterEncoding=utf-8&useSSL=false";

        //加载驱动程序
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url,username,password);
            return conn;
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.error(e.getCause());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * logmon
     * mysql实例信息
     * map类型ip为key
     */
    public static Map<String, MysqlTwoPojo> selectMysqlIpLog(String ip, String schema, String username, String password , Integer type) throws SQLException{
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        String sql = "select \n" +
                "pod_name     \n" +
                ",pod_namespace\n" +
                ",ip           \n" +
                ",UUID         \n" +
                ",zone_code    \n" +
                ",zone_name    \n" +
                ",work_zone    \n" +
                ",product_code \n" +
                ",product_name\n" +
                "from t_logmon_mysqlk8s";
        Map<String, MysqlTwoPojo> MysqlTwoPojoMap = new HashMap<>();
        try {
            conn = getConnection(ip, schema, username, password);
            conn.setAutoCommit(false);

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()){
                MysqlTwoPojo pojo = new MysqlTwoPojo();
                pojo.setProduct_code(rs.getString("product_code"));
                pojo.setProduct_name(rs.getString("product_name"));
                pojo.setWork_zone(rs.getString("work_zone"));
                pojo.setZone_code(rs.getString("zone_code"));
                pojo.setZone_name(rs.getString("zone_name"));
                String hostIp = rs.getString("ip");
                String pod_namespace = rs.getString("pod_namespace");
                String pod_name = rs.getString("pod_name");
                String uuid = rs.getString("uuid");
                pojo.setIp(hostIp);
                pojo.setUuid(uuid);
                pojo.setPod_name(pod_name);
                pojo.setPod_namespace(pod_namespace);
                String instance = pod_namespace +"-"+ pod_name;
                if ( type == 0){
                    MysqlTwoPojoMap.put(hostIp,pojo);
                } else if ( type == 1){
                    MysqlTwoPojoMap.put(instance,pojo);
                } else if ( type == 2){
                    MysqlTwoPojoMap.put(uuid,pojo);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            logger.error(e.getCause());
            e.printStackTrace();
        }finally {
            if (rs != null){
                rs.close();
            }
            if (stmt != null){
                stmt.close();
            }
            if (conn != null){
                conn.close();
            }
        }
        return MysqlTwoPojoMap;
    }

}
