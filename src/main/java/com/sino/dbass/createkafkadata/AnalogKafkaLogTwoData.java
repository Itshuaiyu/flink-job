package com.sino.dbass.createkafkadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sino.dbass.pojo.MysqlTwoPojo;
import com.sino.dbass.utils.CommonUtils;
import com.sino.dbass.utils.ConfigurationManager;
import com.sino.dbass.utils.MysqlUtilsForAnalogData;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

public class AnalogKafkaLogTwoData {

    static String ip = ConfigurationManager.getProperty("mysql.ip.resource");
    static String schema = ConfigurationManager.getProperty("mysql.schema.resource");
    static String username = ConfigurationManager.getProperty("mysql.user");
    static String password = ConfigurationManager.getProperty("mysql.password");
    public static final String broker_list = ConfigurationManager.getProperty("logmon.kafka.bootstrap.servers");
    public static final String topic = ConfigurationManager.getProperty("logmon.kafka.topic");

    public static void main(String[] args) throws Exception{
        makeMysqlk8sData();
    }

    /**
     * mysqlk8s数据
     *
     */
    public static void makeMysqlk8sData() throws Exception{

        List<MysqlTwoPojo> mysqlk8sDatas = MysqlUtilsForAnalogData.selectMysqlTwoForLogmonTwoKafka(ip,schema,username,password);
        Random random = new Random();
        List<JSONObject> logmonPojoList = new ArrayList<>();

        for (MysqlTwoPojo pojo : mysqlk8sDatas) {
            String pod_name = pojo.getPod_name();
            String pod_namespace = pojo.getPod_namespace();
            String uuid = pojo.getUuid();
            String ip = pojo.getIp();

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
            SimpleDateFormat dfTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            //mysql.err日志
            JSONObject jsonMysqlerr = new JSONObject();
            JSONObject defaultJson = new JSONObject();
            defaultJson.put("agentIp",ip);
            defaultJson.put("spMessageTime",new Date().getTime());
            defaultJson.put("agMessage",df.format(new Date())+"\n"+":mysqlk8s mysqlerr cccccc:"+dfTime.format(new Date())
            +"++simulate");
            defaultJson.put("agCollentTime",new Date().getTime() - 150);
            defaultJson.put("agPath","/var/lib/kubelet/pods/"+uuid+
                    "/volumes/kubernetes.io~csi/lvm-bcb5f4e9-1799-4b85-83b6-230e0837735/mount/mysql.err");
            defaultJson.put("spParseTag","MysqlTwo");
            defaultJson.put("spParseRule","MysqlErr");
            defaultJson.put("agSourceTags","MysqlTwo");
            defaultJson.put("agSourceOsVersion","3.10.0-693.11.96.e7.x86_64");

            jsonMysqlerr.put("default",defaultJson);
            JSONObject mysqlJson = new JSONObject();
            mysqlJson.put("time",df.format(new Date()));
            jsonMysqlerr.put("MysqlErr",mysqlJson);
            logmonPojoList.add(jsonMysqlerr);
        }
        CommonUtils.println("创建mysqlk8s日志数据总和:"+logmonPojoList.size());
        insertKafka(logmonPojoList);
    }

    /**
     * 写入kafka
     */
    public static void insertKafka(List<JSONObject> logDatas){
        //属性
        Properties props = new Properties();
        props.put("bootstrap.servers",broker_list);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String,String>(props);

        //批量写入kafka
        for (int i = 0; i < logDatas.size(); i++) {
            ProducerRecord record = new ProducerRecord<String,String>(topic,null,null, JSON.toJSONString(logDatas.get(i)));
            producer.send(record);
        }
        producer.flush();
        producer.close();
        CommonUtils.println("创建kafka的mysqlk8s数据成功!");
    }
}
