package com.sino.dbass.pojo;

import lombok.Data;

@Data
public class MysqlTwoPojo {

    public String product_code;
    public String product_name;
    public String work_zone;
    public String zone_code;
    public String zone_name;
    public String func;
    public String ip;
    public String pod_namespace;
    public String pod_name;
    public String uuid;

    public double metric_value;
    public long _PROCESS_TIME_;
    public String metric_name;
    public String metric_code;
    public String time;
    public String className;

    public MysqlTwoPojo() {
    }
}
