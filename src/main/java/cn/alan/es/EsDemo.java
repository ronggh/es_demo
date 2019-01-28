package cn.alan.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class EsDemo {
    private TransportClient client;


    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws Exception {
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "es").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.154.101"), 9300));

        // 3 打印集群名称
        System.out.println(client.toString());
    }

    @Test
    public void createIndex() {
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();

        // 2 关闭连接
        client.close();
    }


    @Test
    public void deleteIndex() {
        // 1 删除索引
        client.admin().indices().prepareDelete("blog2").get();

        // 2 关闭连接
        client.close();
    }


    // 新建文档（源数据json串）
    @Test
    public void createDocumentByJson() throws Exception {

        // 1 文档数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1")
                .setSource(json).execute().actionGet();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    // 新建文档（源数据map方式添加json）
    @Test
    public void createDocumentByMap() {

        // 1 文档数据准备
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2")
                .setSource(json).execute().actionGet();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //新建文档（源数据es构建器添加json）
    @Test
    public void createDocumentByES() throws Exception {

        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory
                .jsonBuilder()
                .startObject()
                .field("id", 3)
                .field("title", "基于Lucene的搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client
                .prepareIndex("blog", "article", "3")
                .setSource(builder).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }


}
