package com.abecedarian.demo.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by abecedarian on 2019/3/4
 */
public class NativeClientDemo {

    private static Client transportClient = null;

    public static Client build(String esUrl) {
        if (transportClient == null) {
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch").build();
            try {
                transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return transportClient;
    }

    public static void main(String[] args) {

        String index = "demo";
        String type = "native_client";
        String id = "abecedarian";
        String elasticsearchUrl = "http://localhost:9300";
        Client client = build(elasticsearchUrl);

        /**操作索引**/
        try {
            boolean indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
            if (indexExists) {    //存在索引
                client.admin().indices().prepareDelete(index).execute().actionGet();
            }
            Settings settings = Settings.builder()
                    .put("number_of_shards", 5)
                    .put("number_of_replicas", 1)
                    .put("refresh_interval", "1s")
                    .build();

            CreateIndexResponse response = client.admin().indices().prepareCreate(index).setSettings(settings).execute().actionGet();

            if (response.isAcknowledged()) {
                System.out.println("index create success");
            } else {
                System.out.println("index create failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**批量操作文档**/
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println(String.format("bulk create %s success", index));
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println(String.format("bulk create %s fail: %s", index,failure));
            }
        })
                .setBulkActions(-1) //达到批量n请求处理一次
//                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) //达到5M批量处理一次
                .setFlushInterval(TimeValue.timeValueSeconds(5))  //达到5秒刷新一次
                .setConcurrentRequests(1) //设置2个并发处理线程
//                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        JsonObject sourceObj=new JsonObject();
        sourceObj.addProperty("content","native-client");
        sourceObj.addProperty("value","hello elasticsearch");
        sourceObj.addProperty("author","abecedarian");
        System.out.println(new Gson().toJson(sourceObj));
        bulkProcessor.add(new IndexRequest(index, type, id).source(new Gson().toJson(sourceObj), XContentType.JSON));
        bulkProcessor.flush();

        /**查询**/
        GetResponse response=client.prepareGet(index,type,id).get();
        Map<String, Object>  sourceMaps=response.getSource();
        MultiGetResponse multiGetResponse=client.prepareMultiGet().add(index,type).get();
        SearchResponse allHits=client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

        /**关闭客户端**/
//        bulkProcessor.close();
    }


}
