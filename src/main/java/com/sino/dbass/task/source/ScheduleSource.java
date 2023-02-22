package com.sino.dbass.task.source;

import com.sino.dbass.pojo.MysqlTwoPojo;
import com.sino.dbass.task.job.LogMonJob;
import com.sino.dbass.utils.MysqlUtilsForAnalogData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ScheduleSource implements SourceFunction {
    private static final Logger logger = Logger.getLogger(LogMonJob.class);

    private Boolean running = true;
    List<MysqlTwoPojo> mysqlTwoPojos = new ArrayList<>();
    String ip = "192.168.11.101";
    String schema = "flink-sql";
    String username = "root";
    String password = "000000";

    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while (running){
            long time = System.currentTimeMillis();
            logger.info("开始调用数据源数据时间"+time);
            mysqlTwoPojos = MysqlUtilsForAnalogData.selectMysqlTwoForLogmonTwoKafka(ip, schema, username, password);
            sourceContext.collect(mysqlTwoPojos);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
