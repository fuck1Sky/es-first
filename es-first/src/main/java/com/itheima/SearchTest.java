package com.itheima;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchTest {
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
    public void testSearchById() throws Exception{
        //ctrl+alt+v 生成对象
//        1）创建一个client对象
//        2）创建一个查询对象，可以使用QueryString工具类创建QueryString对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2");
//        3）使用client执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello").setTypes("article").setQuery(queryBuilder).get();
//        4）得到查询的结果
        SearchHits searchHits = searchResponse.getHits();
//        5）取查询结果的总记录数
        System.out.println("查询结果总记录数： "+searchHits.getTotalHits());
//        6）取查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            //打印文档对象，以json形式输出
            System.out.println(searchHit.getSourceAsString());
            Map<String, Object> hitSource = searchHit.getSource();
            System.out.println("--------------------------------------------------------------");
            System.out.println(hitSource.get("id"));
            System.out.println(hitSource.get("title"));
            System.out.println(hitSource.get("content"));
        }

//        7）关闭client
        client.close();
    }
}
