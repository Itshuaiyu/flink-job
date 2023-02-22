package com.sino.dbass.task.job;


import com.sino.dbass.task.sink.EsBultSink;
import com.sino.dbass.task.source.ScheduleSource;
import com.sino.dbass.task.transition.SelfToEsTransition;
import com.sino.dbass.utils.CommonUtils;
import com.sino.dbass.utils.ConfigurationManager;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkScheduleSource {
    private static final Logger LOG = Logger.getLogger(FlinkScheduleSource.class);

    public static void main(String[] args) throws Exception {
        String AG_SOURCE_TAGS = ConfigurationManager.getProperty("self.agSourceTags");

        //Parallelism
        int sourcePrlllsm = Integer.parseInt(ConfigurationManager.getProperty("logmon.source.parallelism"));
        int filterPrlllsm = Integer.parseInt(ConfigurationManager.getProperty("logmon.filter.parallelism"));
        int keyByPrlllsm = Integer.parseInt(ConfigurationManager.getProperty("logmon.keyBy.parallelism"));
        int sinkPrlllsm = Integer.parseInt(ConfigurationManager.getProperty("logmon.sink.parallelism"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setRestartStrategy(RestartStrategies.
                fixedDelayRestart(ConfigurationManager.getInteger("logmon.restartAttempts")
                        , Time.of(10, TimeUnit.SECONDS)));
        env.enableCheckpointing(Long.valueOf(ConfigurationManager.getLong("logmon.checkpoint.interval"))
                , CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(Long.valueOf(ConfigurationManager.getLong("logmon.checkpoint.timeout")));
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000 * 5);
        checkpointConfig.setFailOnCheckpointingErrors(false);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //checkpoint path
        String CHECKPOINT_PATH = ConfigurationManager.getProperty("logmon.job.checkpoint");
        //设置checkpoint保存的文件方式
        int backendMode = ConfigurationManager.getInteger("logmon.checkpoint.backend");
        switch (backendMode) {
            case 2:
                env.setStateBackend(new RocksDBStateBackend(CHECKPOINT_PATH));
                break;
            default:
                env.setStateBackend(new FsStateBackend(CHECKPOINT_PATH, true));
        }

        /**
         * ===========================kafka configuration =======================
         */
        String kafkaServers = ConfigurationManager.getProperty("logmon.kafka.bootstrap.servers");
        String kafkaGroupId = ConfigurationManager.getProperty("logmon.kafka.group.id");
        String kafkaTopic = ConfigurationManager.getProperty("logmon.kafka.topic");
        int kafkaConsumerStart = ConfigurationManager.getInteger("logmon.kafka.start");

        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", kafkaServers);
        kafkaProp.put("group.id", kafkaGroupId);

        //FlinkKafkaConsumer kafkaConsumer =
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                Arrays.asList(kafkaTopic.trim().split(",")),
                new SimpleStringSchema(),
                kafkaProp
        );

        switch (kafkaConsumerStart) {
            case 1:
                kafkaConsumer.setStartFromEarliest();
                break;
            case 2:
                kafkaConsumer.setStartFromLatest();
                break;
            default:
                kafkaConsumer.setStartFromGroupOffsets();
        }

        //提交offset到checkpoint上
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        //todo 全局配置
        Configuration globaConfiguration = new Configuration();

        /**
         *==========================MYSQL configuration ==================================
         */
        globaConfiguration.setString("mysql.ip.resource", ConfigurationManager.getProperty("mysql.ip.resource"));
        globaConfiguration.setString("mysql.schema.resource", ConfigurationManager.getProperty("mysql.schema.resource"));
        globaConfiguration.setString("mysql.automation.interval", ConfigurationManager.getProperty("mysql.automation.interval"));
        globaConfiguration.setString("mysql.user", ConfigurationManager.getProperty("mysql.user"));
        globaConfiguration.setString("mysql.password", ConfigurationManager.getProperty("mysql.password"));

        /**
         * =========================es configuration ==================================
         */
        //es参数配置
        globaConfiguration.setString("Elasticsearch.hosts", ConfigurationManager.getProperty("Elasticsearch.hosts"));
        globaConfiguration.setString("bulk.flush.max.actions", ConfigurationManager.getProperty("bulk.flush.max.actions"));
        globaConfiguration.setString("bulk.flush.max.size.mb", ConfigurationManager.getProperty("bulk.flush.max.size.mb"));
        globaConfiguration.setString("bulk.concurrent.Requests", ConfigurationManager.getProperty("bulk.concurrent.Requests"));
        globaConfiguration.setString("bulk.flush.interval.ms", ConfigurationManager.getProperty("bulk.flush.interval.ms"));
        globaConfiguration.setString("bulk.flush.backoff.delay", ConfigurationManager.getProperty("bulk.flush.backoff.delay"));
        globaConfiguration.setString("bulk.flush.backoff.retries", ConfigurationManager.getProperty("bulk.flush.backoff.retries"));
        globaConfiguration.setString("bulk.elasticsearch.countandtime", ConfigurationManager.getProperty("bulk.elasticsearch.countandtime"));

        //es的index情况
        globaConfiguration.setString("logmon.elasticsearch.sink.oraclepro.index", ConfigurationManager.getProperty("logmon.elasticsearch.sink.oraclepro.index"));
        globaConfiguration.setString("logmon.elasticsearch.sink.oraclecloud.index", ConfigurationManager.getProperty("logmon.elasticsearch.sink.oraclecloud.index"));
        globaConfiguration.setString("logmon.elasticsearch.sink.mysqlk8s.index", ConfigurationManager.getProperty("logmon.elasticsearch.sink.mysqlk8s.index"));

        //打印设置
        globaConfiguration.setBoolean("check.System.out.tran.println", ConfigurationManager.getBoolean("check.System.out.tran.println"));
        globaConfiguration.setBoolean("check.System.out.sink.println", ConfigurationManager.getBoolean("check.System.out.sink.println"));
        Boolean checkMainPrintln = ConfigurationManager.getBoolean("check.System.out.main.println");

        CommonUtils.printlnMain(globaConfiguration, checkMainPrintln);

        env.getConfig().setGlobalJobParameters(globaConfiguration);


        /**
         * ===============================================解析自定义数据源数据================================================
         */
        SingleOutputStreamOperator selfSource = env.addSource(new ScheduleSource())
                .name("SelfSource")
                .uid("SelfSource")
                .setParallelism(sourcePrlllsm);


/*

        SingleOutputStreamOperator readStreamEs = selfSource.process()
                .name("ESDataHandle")
                .uid("ESDataHandle")
                .setParallelism(keyByPrlllsm);
*/

        /**
         * ======================================侧输出流=======================================
         */
        //todo 定义一个侧输出流OutputTag,类型是String
        OutputTag mysqlk8sOutputTag = new OutputTag<String>("mysqlk8sOutputTag", TypeInformation.of(String.class)) {
        };

      /*  readStreamEs.getSideOutput(mysqlk8sOutputTag)
                .addSink(new EsBultSink())
                .uid("Mysql_ElasticsearchSink")
                .name("Mysql_ElasticsearchSink")
                .setParallelism(sinkPrlllsm);
*/
        if (args.length == 0) {
            env.execute(ConfigurationManager.getProperty("logmon.job.name"));
        } else {
            env.execute(args[0]);
        }
    }
}
