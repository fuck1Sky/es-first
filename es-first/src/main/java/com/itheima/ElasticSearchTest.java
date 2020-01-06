package com.itheima;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchTest {

    private TransportClient client;

    @Before
    public void init() throws Exception{
        // 创建Client连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }

    @Test
    //创建索引
    public void createIndex() throws Exception{
        // 创建Client连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        //创建名称为blog2的索引
        client.admin().indices().prepareCreate("index_hello").get();
        //释放资源
        client.close();
    }

    @Test
    //创建映射
    public void setMappings() throws Exception{
        // 创建Client连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        // 添加映射
        /**
         * 格式：
         * "mappings" : {
         "article" : {
         "dynamic" : "false",
         "properties" : {
         "id" : { "type" : "string" },
         "content" : { "type" : "string" },
         "author" : { "type" : "string" }
         }
         }
         }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long").field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text").field("store", true).field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text").field("store", true).field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        // 创建映射
        PutMappingRequest mapping = Requests.putMappingRequest("index_hello")
                .type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();
        //释放资源
        client.close();
    }

    @Test
    //创建文档
    public void testAddDocument() throws Exception{
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",2)
                    .field("title","离开洒家覅文件欧服美服雷克萨解放军领导222")
                    .field("content","开始就覅Jeri u饿哦日军端口发送旅客的结果222")
                .endObject();
        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId("2")
                //设置文档信息
                .setSource(builder).get();
        client.close();
    }


    @Test
    //创建文档2
    public void testAddDocument2() throws Exception{

        //创建article对象
        Article article = new Article();
        //设置对象属性
        article.setId(3);
        article.setTitle("iu哦而同年，没人难题u的覅u呢融入时间内如恶化乳房3");
        article.setContent("忙不忙呢我本人问问任务和史蒂夫纳什模块3");
        //把对象转换成json
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonObject = objectMapper.writeValueAsString(article);

        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId("3")
                //设置文档信息
                .setSource(jsonObject, XContentType.JSON).get();
        client.close();
    }



}
