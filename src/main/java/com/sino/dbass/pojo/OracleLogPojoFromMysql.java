package com.sino.dbass.pojo;

import lombok.Data;

@Data
public class OracleLogPojoFromMysql {

    public String product_code;
    public String product_name;
    public String work_zone;
    public String zone_code;
    public String ip;
    public String zone_name;
    public String instance_name;
    public String hostname;

    //仅oraclecould有
    public String cdbName;

    //仅oraclepro有
    public String dbName;
    public String racName;
    public String serviceName;

    public OracleLogPojoFromMysql() {
    }
}
