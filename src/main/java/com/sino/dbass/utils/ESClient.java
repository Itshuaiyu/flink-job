package com.sino.dbass.utils;

import com.sino.dbass.task.sink.EsBultSink;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ESClient {

    private static ESClient ESClients;
    private String host = "127.0.0.1:9200";
    private RestClientBuilder builder;
    private static RestHighLevelClient highClient;

    public ESClient() {
    }

    public static ESClient getInstance() {
        if (ESClients == null) {
            synchronized (ESClient.class) {
                if (ESClients == null) {
                    ESClients = new ESClient();
                    ESClients.initBuilder();
                }
            }
        }
        return ESClients;
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
            synchronized (ESClient.class) {
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
    public void closeClient(){
        if (null != highClient){
            try {
                highClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static RestHighLevelClient getClient(String host,String port){
        //创建httpHost对象
        HttpHost httpHost = new HttpHost(host, Integer.parseInt(port));
        //创建RestClientBuilder
        RestClientBuilder builder = RestClient.builder(httpHost);
        //创建RestHighLevelClient对象
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

}
