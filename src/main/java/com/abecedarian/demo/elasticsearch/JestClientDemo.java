package com.abecedarian.demo.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * Created by abecedarian on 2019/3/8
 */
public class JestClientDemo {

    private final static int SOCKET_READ_TIME_OUT = 80000;
    private static JestClient jestClient;


    public static JestClient build(String esUrl) {
        if (jestClient == null) {
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig.Builder(esUrl)
                    .multiThreaded(true)
                    .readTimeout(SOCKET_READ_TIME_OUT)
                    .build()
            );
            jestClient = factory.getObject();
        }
        return jestClient;
    }


    public static void main(String[] args) throws IOException {

        String index = "demo";
        String type = "jest_client";
        String id = "abecedarian";
        String elasticsearchUrl = "http://localhost:9200";
        JestClient client = build(elasticsearchUrl);

        /**操作索引**/
        try {
            IndicesExists.Builder builder = new IndicesExists.Builder(index);
            JestResult indexExistsResult = client.execute(builder.build());

            if (indexExistsResult.isSucceeded()) {    //存在索引
                client.execute(new DeleteIndex.Builder(index).build());
            }
            Settings settings = Settings.builder()
                    .put("number_of_shards", 6)
                    .put("number_of_replicas", 1)
                    .put("refresh_interval", "1s")
                    .build();

            JestResult jestResult = client.execute(new CreateIndex.Builder(index).settings(settings.getAsStructuredMap()).build());
            if (jestResult.isSucceeded()) {
                System.out.println("index create success");
            } else {
                System.out.println("index create failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**批量操作文档**/
        try {
            JsonObject sourceObj = new JsonObject();
            sourceObj.addProperty("content", "native-client");
            sourceObj.addProperty("value", "hello elasticsearch");
            sourceObj.addProperty("author", "abecedarian");
            Bulk.Builder builder = new Bulk.Builder();
            Index idx = new Index.Builder(new Gson().toJson(sourceObj)).index(index).type(type).id(id).build();
            builder.addAction(idx);
            JestResult bulkResult = client.execute(builder.build());
            if (bulkResult.isSucceeded()) {
                System.out.println(String.format("bulk create %s success", index));
            } else {
                System.out.println(String.format("bulk create %s failed", index));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**查询**/
        try {
            String query = "{\n" +
                    "  \"query\":{\n" +
                    "    \"match\":{\n" +
                    "      \"key\":\"hello\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            Search.Builder searchBuilder = new Search.Builder(query).addIndex(index).addType(type);
            SearchResult searchResult = client.execute(searchBuilder.build());

            List<SearchResult.Hit<JsonObject, Void>> hits = searchResult.getHits(JsonObject.class);
            for (SearchResult.Hit<JsonObject, Void> hit : hits) {
                JsonObject obj = hit.source;
                System.out.println(new Gson().toJson(obj));
            }
            if (searchResult.isSucceeded()) {
                System.out.println("search query success");
            } else {
                System.out.println("search query fail");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /**关闭客户端**/
        client.close();
    }
}
