package cn.alan.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;



import java.net.InetAddress;

public class EsDemo {
    private  TransportClient client;


    @SuppressWarnings("unchecked")
    @Before
    public  void getClient() throws Exception {
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "es").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.154.101"), 9300));

        // 3 打印集群名称
        System.out.println(client.toString());
    }

    @Test
    public void createIndex(){
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();

        // 2 关闭连接
        client.close();
    }
    @Test
    public void deleteIndex(){
        // 1 删除索引
        client.admin().indices().prepareDelete("blog2").get();

        // 2 关闭连接
        client.close();
    }

}
