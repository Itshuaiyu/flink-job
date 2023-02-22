package com.sino.dbass.task.transition;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sino.dbass.pojo.MysqlTwoPojo;
import com.sino.dbass.utils.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SingleDataToEsTransition extends KeyedProcessFunction<Integer, String, String> {
    private static Logger LOG = Logger.getLogger(SingleDataToEsTransition.class);

    //mysql基本信息
    String mysql_ip_resource = "";
    String mysql_schema_resource = "";
    String username = "";
    String password = "";

    Map<String, MysqlTwoPojo> stringMysqlTwoIpPojoMap = null;
    Map<String, MysqlTwoPojo> stringMysqlTwoPojoMap = null;
    Map<String, MysqlTwoPojo> stringMysqlTwoUuidPojoMap = null;
    //最终处理结果的数据
    Map<String, Object> resultDataMap = null;

    List<String> ipList = new ArrayList<>();

    long mysql_automation_interval = 0;

    //es索引信息
    String mysqlTwo_log_index = "";

    //原始日志字段值变量
    String spParseRule = "";
    String agentIp = "";
    String agSourceTags = "";
    String agPath = "";
    String logType = "";
    String dbType = "";
    String logOwnen = "";
    String logFileName = "";
    String logTime = "";
    long agCollectTime = 0;
    //初始化注册定时器
    boolean needRegisterTimer = true;
    boolean checkTranPrintln = false;

//    OutputTag mysqlk8sOutputTag = new OutputTag<String>("mysqlk8sOutputTag", TypeInformation.of(String.class)) {
//    };


    /**
     * ===================================获取mysql指标数据=================================
     */
    public void getMysqlDatas() throws SQLException {

        //获取mysql实例ip基本过滤数据
        stringMysqlTwoIpPojoMap = MysqlUtils.selectMysqlIpLog(mysql_ip_resource, mysql_schema_resource, username, password, 0);
        LOG.debug("stringMysqlTwoIpPojoMap===" + stringMysqlTwoIpPojoMap);
        CommonUtils.printlnTran("stringMysqlTwoIpPojoMap===" + JSON.toJSON(stringMysqlTwoIpPojoMap), checkTranPrintln);

        //获取mysql实例 通过uuid组成map
        stringMysqlTwoUuidPojoMap = MysqlUtils.selectMysqlIpLog(mysql_ip_resource, mysql_schema_resource, username, password, 2);
        LOG.debug("stringMysqlTwoUuidPojoMap===" + stringMysqlTwoUuidPojoMap);
        CommonUtils.printlnTran("stringMysqlTwoUuidPojoMap===" + JSON.toJSON(stringMysqlTwoUuidPojoMap), checkTranPrintln);

        //获取mysql实例 通过podname和podnamespace组成map
        stringMysqlTwoPojoMap = MysqlUtils.selectMysqlIpLog(mysql_ip_resource, mysql_schema_resource, username, password, 1);
        LOG.debug("stringMysqlTwoPojoMap===" + stringMysqlTwoPojoMap);
        CommonUtils.printlnTran("stringMysqlTwoPojoMap===" + JSON.toJSON(stringMysqlTwoPojoMap), checkTranPrintln);

        ipList.addAll(stringMysqlTwoIpPojoMap.keySet());
        LOG.info("=========get mysql data=================");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globalConf = (Configuration) globalJobParameters;
        //mysql重新获得数据间隔
        mysql_automation_interval = globalConf.getLong("mysql.automation.interval", 300000);

        //mysql配置信息
        mysql_ip_resource = globalConf.getString("mysql.ip.resource", "localhost");
        mysql_schema_resource = globalConf.getString("mysql.schema.resource", "flink-sql");
        username = globalConf.getString("mysql.user", "root");
        password = globalConf.getString("mysql.password", "000000");

        //elasticsearch的index信息
        mysqlTwo_log_index = globalConf.getString("logmon.elasticsearch.sink.mysqlk8s.index", "");
        getMysqlDatas();
    }

    /**
     * 定时获取logmonn
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        LOG.debug("定时器触发：" + timestamp);

        if (ipList != null) {
            ipList.clear();
        }
        if (stringMysqlTwoIpPojoMap != null) {
            stringMysqlTwoIpPojoMap.clear();
            stringMysqlTwoIpPojoMap = null;
        }

        getMysqlDatas();
        LOG.debug("再次注册定时器：" + timestamp);
        ctx.timerService().registerProcessingTimeTimer(timestamp + mysql_automation_interval);
        LOG.debug("定时器执行完毕\n");
    }

    @Override
    public void processElement(String sourceData, Context context, Collector<String> collector) throws Exception {
        //验证数据是否可用，是否需要保存到es中
        boolean isDivision = false;
        //初始化注册定时器
        if (needRegisterTimer) {
            long currentTime = System.currentTimeMillis();
            context.timerService().registerProcessingTimeTimer(currentTime + mysql_automation_interval);
            needRegisterTimer = false;
        }
        //初始化解析数据
        resultDataMap = analysisSourceData(sourceData);
        //获取ip信息
        agentIp = resultDataMap.get("agentIp").toString();
        if (ipList.contains(agentIp)) {
            //路径
            agPath = IsAvailableUtil.isObjectAvailable(resultDataMap.get("agPath")) ? resultDataMap.get("agPath").toString() : "";
            //采集规则的标签
            agSourceTags = resultDataMap.get("agSourceTags").toString();
            //通过路径解析日志类型
            getLogType();

            /**
             * mysql日志处理
             *
             */
            if (agSourceTags.toLowerCase().contains("mysqltwo")) {
                isDivision = true;
                dbType = "mysqlTwo";
                analysisiMysqlLogPath();
                MysqlTwoPojo mysqlData = stringMysqlTwoPojoMap.get(resultDataMap.get("logOwner"));
                if (IsAvailableUtil.isObjectAvailable(mysqlData)) {
                    resultDataMap.put("product_code", mysqlData.getProduct_code());
                    resultDataMap.put("product_name", mysqlData.getProduct_name());
                    resultDataMap.put("work_zone", mysqlData.getWork_zone());
                    resultDataMap.put("zone_code", mysqlData.getZone_code());
                    resultDataMap.put("zone_name", mysqlData.getZone_name());
                    resultDataMap.put("pod_name", mysqlData.getPod_name());
                    resultDataMap.put("pod_namespace", mysqlData.getPod_namespace());
                    resultDataMap.put("uuid", mysqlData.getUuid());
                } else {
                    CommonUtils.printlnTran("mysqlk8s数据不存在：" + JSON.toJSON(resultDataMap), checkTranPrintln);
                    isDivision = false;
                }
            }

            if (isDivision) {
                JSONObject resultEsDataMap = new JSONObject();
                //共同属性
                resultEsDataMap.put("agCollectTime", resultDataMap.get("agCollectTime"));
                resultEsDataMap.put("agMessage", resultDataMap.get("agMessage"));
                resultEsDataMap.put("agPath", resultDataMap.get("agPath"));
                resultEsDataMap.put("agSourceOsVersion", resultDataMap.get("agSourceOsVersion"));
                resultEsDataMap.put("agSourceTags", resultDataMap.get("agSourceTags"));
                resultEsDataMap.put("agentIp", resultDataMap.get("agentIp"));
                resultEsDataMap.put("agentName", resultDataMap.get("agentName"));
                resultEsDataMap.put("logFileName", resultDataMap.get("logFileName"));
                resultEsDataMap.put("logOwner", resultDataMap.get("logOwner"));
                resultEsDataMap.put("logType", resultDataMap.get("logType"));
                resultEsDataMap.put("spParseRule", resultDataMap.get("spParseRule"));
                resultEsDataMap.put("spParseTag", resultDataMap.get("spParseTag"));

                //格式化时间 方便查看
                resultEsDataMap.put("time", resultDataMap.get("agCollectTime"));
                resultEsDataMap.put("product_code", resultDataMap.get("product_code"));
                resultEsDataMap.put("product_name", resultDataMap.get("product_name"));
                resultEsDataMap.put("work_zone", resultDataMap.get("work_zone"));
                resultEsDataMap.put("zone_code", resultDataMap.get("zone_code"));
                resultEsDataMap.put("zone_name", resultDataMap.get("zone_name"));

                //同一时间同一个实例不能是同一个日志信息
                String id = (dbType + resultDataMap.get("logOwner") + resultDataMap.get("agentIp")
                        + resultDataMap.get("agMessage").hashCode() + "" + resultDataMap.get("agCollectTime")
                );
                resultEsDataMap.put("id", id);

                //私有属性
                switch (dbType) {
                    case "mysqlTwo":
                        resultEsDataMap.put("pod_name", resultDataMap.get("pod_name"));
                        resultEsDataMap.put("pod_namespace", resultDataMap.get("pod_namespace"));
                        resultEsDataMap.put("uuid", resultDataMap.get("uuid"));
                        resultEsDataMap.put("index", mysqlTwo_log_index);
                        //context.output(mysqlk8sOutputTag, resultEsDataMap.toString());
                        collector.collect(String.valueOf(resultEsDataMap));
                        break;
                    default:
                        break;
                }
            }
            resultDataMap = null;
        } else {
            LOG.debug("该数据不作解析，IP不是需要解析数据库：" + sourceData);
        }
    }

    /**
     * 解析日志路径
     */
    private void analysisiMysqlLogPath() {
        if (resultDataMap.get("logType").equals("Mysql-Error")) {
            String[] urls = agPath.split("/");
            String uuid = urls[5];
            MysqlTwoPojo mysqlUuidTwoPojo = stringMysqlTwoUuidPojoMap.get(uuid);
            if (IsAvailableUtil.isObjectAvailable(mysqlUuidTwoPojo)) {
                logOwnen = mysqlUuidTwoPojo.getPod_namespace() + "-" + mysqlUuidTwoPojo.getPod_name();
            }
            logFileName = urls[10];
        } else if (resultDataMap.get("logType").equals("Mysql-slow-query")) {
            logOwnen = StringsUtil.getStrFromStr2(agPath, "/", 1);
            logFileName = StringsUtil.getStrFromStr2(agPath, "/", 2);
        } else {
            logOwnen = "";
            logFileName = "";
        }
        LOG.debug("logFileName:" + logFileName);
        resultDataMap.put("logOwner", logOwnen);
        resultDataMap.put("logFileName", logFileName);
    }

    /**
     * 获取日志具体类型
     */
    private void getLogType() {
        if (agPath.contains("mysql.err")) {
            logType = "Mysql-Error";
            resultDataMap.put("logType", logType);
        } else if (agPath.contains("slow-query.log")) {
            logType = "Mysql-slow-query";
            resultDataMap.put("logType", logType);
        }
    }

    /**
     * 解析元数据
     */
    public Map<String, Object> analysisSourceData(String sourceData) {
        JSONObject jsonObject = JSON.parseObject(sourceData);
        JSONObject defaultJSONObject = jsonObject.getJSONObject("default");
        //获取解析规则，判断是否为空
        Object rule = defaultJSONObject.get("spParseRule");
        spParseRule = IsAvailableUtil.isObjectAvailable(rule) ? rule.toString() : "";
        logTime = jsonObject.getJSONObject(spParseRule).getString("time");
        LocalDateTime localDateTime = DateParseAndFormat.strParseToLocalDateTime(logTime);
        Date date = DateParseAndFormat.localDateTimeToDate(localDateTime);
        try {
            agCollectTime = date.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<String, Object> resultMap = (Map<String, Object>) defaultJSONObject;
        if (agCollectTime != 0){
            resultMap.put("agCollectTime",agCollectTime);
        }
        return resultMap;
    }
}
