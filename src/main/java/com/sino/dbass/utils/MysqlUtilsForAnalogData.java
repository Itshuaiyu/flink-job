package com.sino.dbass.utils;


import com.sino.dbass.pojo.MysqlTwoPojo;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.core.config.Scheduled;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlUtilsForAnalogData {
    private static Logger logger = Logger.getLogger(MysqlUtilsForAnalogData.class);

    public static Connection getConnection(String ip, String schema, String username, String password){
        String url = "jdbc:mysql://" + ip + "/" +schema + "?characterEncoding=utf-8&useSSL=false";

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url,username,password);
            return conn;
        } catch (Exception e) {
            logger.error(e.getCause());
            logger.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    /**
     * mysqlk8s实例数据
     */
    public static List<MysqlTwoPojo> selectMysqlTwoForLogmonTwoKafka(String ip,String schema,String username,String password)
    throws SQLException{
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
        List<MysqlTwoPojo> mysqlk8sPojoList = new ArrayList<>();

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
                pojo.setIp(rs.getString("ip"));
                pojo.setUuid(rs.getString("uuid"));
                pojo.setPod_name(rs.getString("pod_name"));
                pojo.setPod_namespace(rs.getString("pod_namespace"));
                mysqlk8sPojoList.add(pojo);
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
        return mysqlk8sPojoList;
    }
}
