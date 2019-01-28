package cn.alan.es;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EsDemo {
    private TransportClient client;


    /**
     * 获取ES 客户端连接
     *
     * @throws Exception
     */
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

    /**
     * 创建Index
     */
    @Test
    public void createIndex() {
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();

        // 2 关闭连接
        client.close();
    }

    /**
     * 删除Index
     */
    @Test
    public void deleteIndex() {
        // 1 删除索引
        client.admin().indices().prepareDelete("blog2").get();

        // 2 关闭连接
        client.close();
    }


    /**
     * 新建文档，方式一：源数据json串格式
     */
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

    /**
     * 新建文档，方式二：源数据map方式添加json
     */
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

    /**
     * 新建文档，方式三：源数据es构建器添加json
     */
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

    /**
     * 搜索文档数据（单个索引）
     *
     * @throws Exception
     */
    @Test
    public void getData() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog", "article", "1").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        // 3 关闭连接
        client.close();
    }

    /**
     * 搜索文档数据（多个索引）
     */
    @Test
    public void getMultiData() {

        // 1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2", "3")
                .add("blog", "article", "2")
                .get();

        // 2 遍历返回的结果
        for (MultiGetItemResponse itemResponse : response) {
            GetResponse getResponse = itemResponse.getResponse();

            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        // 3 关闭资源
        client.close();
    }

    /**
     * 更新文档数据-update
     *
     * @throws Throwable
     */
    @Test
    public void updateDocument() throws Throwable {

        // 1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("3");

        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .field("createDate", "2017-8-22")
                .endObject());

        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    /**
     * 更新文档数据（upsert）,没有则添加，有则更新
     *
     * @throws Exception
     */
    @Test
    public void upsertDocument() throws Exception {

        // 1. 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blog", "article", "5")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "搜索服务器")
                        .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                        .endObject());

        // 2. 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blog", "article", "5")
                .doc(XContentFactory.jsonBuilder().
                        startObject()
                        .field("user", "李四")
                        .endObject())
                .upsert(indexRequest);

        // 3. 更新文档
        UpdateResponse response = client.update(upsert).get();

        // 4. 打印返回的结果
        System.out.println("index:" + response.getIndex());
        System.out.println("type:" + response.getType());
        System.out.println("id:" + response.getId());
        System.out.println("version:" + response.getVersion());
        System.out.println("create:" + response.getResult());

        // 5. 关闭连接
        client.close();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteData() {

        // 1 删除文档数据
        DeleteResponse response = client.prepareDelete("blog", "article", "5").get();

        // 2 打印返回的结果
        System.out.println("index:" + response.getIndex());
        System.out.println("type:" + response.getType());
        System.out.println("id:" + response.getId());
        System.out.println("version:" + response.getVersion());
        System.out.println("found:" + response.getResult());

        // 3 关闭连接
        client.close();
    }

    /**
     * 查询所有（matchAllQuery）
     */
    @Test
    public void matchAllQuery() {

        // 1 执行查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }

        // 3 关闭连接
        client.close();
    }

    /**
     * 对所有字段分词查询（queryStringQuery）
     */
    @Test
    public void query() {
        // 1 条件查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("全文")).get();

        // 2 打印查询结果
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }

    /**
     * 通配符查询（wildcardQuery）
     */
    @Test
    public void wildcardQuery() {
        // 1 通配符查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*全*"))
                .get();

        // 2 打印查询结果
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }

    /**
     * 词条查询
     */
    @Test
    public void termQuery() {
        // 1  field查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "全"))
                .get();

        // 2 打印查询结果
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }


    /**
     *  模糊查询
     */
    @Test
    public void fuzzy() {
        // 1 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "lucene"))
                .get();

        // 2 打印查询结果
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }

    /**
     * 映射操作
     */
    @Test
    public void createMapping() throws Exception {
        // 1.设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
           .startObject()
              .startObject("article")
                    .startObject("properties")
                        // id1
                        .startObject("id1")
                            .field("type", "string")
                            .field("store", "yes")
                        .endObject()
                        // title2
                        .startObject("title2")
                            .field("type", "string")
                            .field("store", "no")
                        .endObject()
                        // content
                        .startObject("content")
                            .field("type", "string")
                            .field("store", "yes")
                        .endObject()
                    .endObject()
               .endObject()
            .endObject();

        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog4")
                .type("article").source(builder);

        // 需要先创建出索引，否则会出错
        client.admin().indices().prepareCreate("blog4").get();

        // 创建映射，如果已存在，会出错
        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();
    }

}
