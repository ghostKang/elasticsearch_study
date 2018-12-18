package com.yk.elasticsearchstudy;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Yuk on 2018/12/17.
 */
@Configuration
public class MyConfig {

    @Bean
    public TransportClient client() throws UnknownHostException{

        InetSocketTransportAddress node = new InetSocketTransportAddress(
                InetAddress.getByName("localhost"),// 节点地址
                9300// TCP端口，不是http端口9200
        );

        Settings settings = Settings.builder()
                .put("cluster.name","yukang")// 节点名
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddresses(node);// 找到节点,可添加多个
        return client;
    }
}
