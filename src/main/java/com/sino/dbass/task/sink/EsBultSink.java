package com.sino.dbass.task.sink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.log.NullLogger;
import com.sino.dbass.utils.CommonUtils;
import com.sino.dbass.utils.ESClient;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;

/**
 * 自定义批量保存es数据
 */
public class EsBultSink extends RichSinkFunction<String> implements SinkFunction<String> {
    private static Logger logger = Logger.getLogger(EsBultSink.class);

    private static Configuration globalConf = null;
    private static BulkProcessor bulkProcessor = null;
    private static RestHighLevelClient client = null;
    private static BulkProcessor.Listener listener = null;
    private static BulkProcessor.Builder bulk = null;

    private static boolean checkContAndTime = true;
    private static boolean checkSinkPrintln = false;

    private static String host = "";

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig.GlobalJobParameters globalConfiguration = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        globalConf = (Configuration) globalConfiguration;

        checkContAndTime = globalConf.getBoolean("bulk.elasticsearch.countandtime", true);
        checkSinkPrintln = globalConf.getBoolean("check.System.out.sink.println", true);
        host = globalConf.getString("Elasticsearch.hosts", "0.0.0.0");

        CommonUtils.printlnSink(host, checkSinkPrintln);
        CommonUtils.printlnSink("globalConf:" + JSON.parseObject(JSON.toJSONString(globalConf.toMap())), checkSinkPrintln);

        //BulkProcessor是一个线程安全的批量处理类，允许方便的设置 刷新 一个新的批量请求
        client = new ESClient().getInstance().getHighLevelClient();
        listener = buildListener();
        bulk = BulkProcessor.builder(
                (request, bulkListener) ->
                        EsBultSink.client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener);
        //根据当前添加的操作数设置刷新新批量请求的时间（默认值为1000，-1禁用）
        bulk.setBulkActions(globalConf.getInteger("bulk.flush.max.actions", 1000));
        //根据当前添加的操作大小设置刷新新批量请求的时间（默认为5Mb,-1禁用）
        bulk.setBulkSize(new ByteSizeValue(globalConf.getInteger("bulk.flush.max.size.mb", 5), ByteSizeUnit.MB));
        //设置允许执行的并发请求数（默认为1，0仅允许执行单个请求）
        bulk.setConcurrentRequests(globalConf.getInteger("bulk.concurrent.Requests", 1));
        //设置一个刷新间隔，如果间隔过去，刷新任何待处理的批量请求（默认为未设置）
        bulk.setFlushInterval(TimeValue.timeValueMillis(globalConf.getInteger("bulk.flush.interval.ms", 3000)));
        //设置一个恒定的后退策略，最初等待1秒钟，最多重试3次
        bulk.setBackoffPolicy(BackoffPolicy.constantBackoff(
                TimeValue.timeValueSeconds(globalConf.getInteger("bulk.flush.backoff.delay", 1000)),
                globalConf.getInteger("bulk.flush.backoff.retries", 3)));
        bulkProcessor = bulk.build();
        super.open(parameters);
    }

    private static BulkProcessor.Listener buildListener() throws InterruptedException {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (checkContAndTime) {
                    CommonUtils.printlnSink("存储es条数："
                            + bulkRequest.numberOfActions()
                            + "消耗时间：" + bulkResponse.getTook()
                            + "索引信息：" + bulkRequest.getDescription(), checkSinkPrintln);
                    bulkRequest.getDescription();
                    List<DocWriteRequest<?>> requestsDatas = bulkRequest.requests();
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

                CommonUtils.printlnSink("添加失败" + bulkRequest.numberOfActions(), checkSinkPrintln);
                List<DocWriteRequest<?>> requestsDatas = bulkRequest.requests();

                try {
                    BulkProcessor.Listener listenerAfter = buildListenerAfter();
                    BulkProcessor.Builder bulkAfter = BulkProcessor.builder(
                            (request, bulkListener) ->
                                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                            listenerAfter);
                    //根据当前添加的操作数设置刷新新批量请求的时间（默认值为1000，-1禁用）
                    bulkAfter.setBulkActions(globalConf.getInteger("bulk.flush.max.actions", 1000));
                    //根据当前添加的操作大小设置刷新新批量请求的时间（默认为5Mb,-1禁用）
                    bulkAfter.setBulkSize(new ByteSizeValue(globalConf.getInteger("bulk.flush.max.size.mb", 5), ByteSizeUnit.MB));
                    //设置允许执行的并发请求数（默认为1，0仅允许执行单个请求）
                    bulkAfter.setConcurrentRequests(globalConf.getInteger("bulk.concurrent.Requests", 1));
                    //设置一个刷新间隔，如果间隔过去，刷新任何待处理的批量请求（默认为未设置）
                    bulkAfter.setFlushInterval(TimeValue.timeValueMillis(globalConf.getInteger("bulk.flush.interval.ms", 3000)));
                    //设置一个恒定的后退策略，最初等待1秒钟，最多重试3次
                    bulkAfter.setBackoffPolicy(BackoffPolicy.constantBackoff(
                            TimeValue.timeValueSeconds(globalConf.getInteger("bulk.flush.backoff.delay", 1000)),
                            globalConf.getInteger("bulk.flush.backoff.retries", 3)));

                    BulkProcessor bulkProcessorAfter = bulkAfter.build();
                    for (int i = 0; i < requestsDatas.size(); i++) {
                        DocWriteRequest<?> docWriteRequest = requestsDatas.get(i);
                        bulkProcessorAfter.add(docWriteRequest);
                    }
                    bulkProcessorAfter.flush();

                    bulkProcessorAfter.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        return listener;
    }

    private static BulkProcessor.Listener buildListenerAfter() throws InterruptedException {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                bulkProcessor.flush();
                if (checkContAndTime) {
                    CommonUtils.printlnSink("newnewnew==success==存储es条数："
                            + bulkRequest.numberOfActions()
                            + ",消耗时间："
                            + bulkResponse.getTook()
                            + "索引信息：" + bulkRequest.getDescription(), checkSinkPrintln);
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                CommonUtils.printlnSink("newnewnew==添加失败"
                        + bulkRequest.numberOfActions(), checkSinkPrintln);
                List<DocWriteRequest<?>> requestsDatas = bulkRequest.requests();
            }
        };
        return listener;
    }

    @Override
    public void invoke(String elementStr, Context context) throws Exception {
        try {
            JSONObject element = JSONObject.parseObject(elementStr);
            String index = element.getString("index");
            String id = element.getString("id");

            element.remove("index");
            element.remove("id");

            bulkProcessor.add(new IndexRequest(index)
                    .id(id)
                    .source(element));
        } catch (Exception e) {
            // todo 防止别名出错，若添加数据时，别名不存在，校验当前日期的index是否存在，存在的话添加别名，不存在，添加新的index
            CommonUtils.printlnSink("sink出错 {{}},消息是{{}}" + elementStr, checkSinkPrintln);
            logger.error(e.getMessage());
            logger.error("sink出错 {{}},消息是{{}}" + elementStr);
        }
    }

    class ESClient {
        private EsBultSink.ESClient ESClient;
        private RestClientBuilder builder;
        private RestHighLevelClient highClient;

        private ESClient() {
        }

        public EsBultSink.ESClient getInstance() {
            if (ESClient == null) {
                synchronized (com.sino.dbass.utils.ESClient.class) {
                    if (ESClient == null) {
                        ESClient = new ESClient();
                        ESClient.initBuilder();
                    }
                }
            }
            return ESClient;
        }

        public RestClientBuilder initBuilder() {
            String[] hosts = host.split(",");
            HttpHost[] httpHosts = new HttpHost[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                String[] host = hosts[i].split(":");
                httpHosts[i] = new HttpHost(host[0], Integer.parseInt(host[1]), "http");
            }

            builder = RestClient.builder(httpHosts);

            //region 在Bulider中设置请求头
            //1.设置请求头
            Header[] defaultHeaders = {
                    new BasicHeader("Content-Type", "application/json")
            };
            builder.setDefaultHeaders(defaultHeaders);
            return builder;
        }

        public RestHighLevelClient getHighLevelClient() {
            if (highClient == null) {
                synchronized (com.sino.dbass.utils.ESClient.class) {
                    if (highClient == null) {
                        highClient = new RestHighLevelClient(builder);
                    }
                }
            }
            return highClient;
        }

        /**
         * 关闭sniffer client
         */
        public void closeClient() {
            if (null != highClient) {
                try {
                    highClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
