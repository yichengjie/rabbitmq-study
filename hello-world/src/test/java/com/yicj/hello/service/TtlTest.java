package com.yicj.hello.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
public class TtlTest extends BaseJunitClz {

    /**
     * 队列的过期时间
     */
    @Test
    public void queueExpires() throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        //
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        // 申明队列的过期时间
        Map<String, Object> args = new HashMap<>(16) ;
        args.put("x-expires", 1800000) ;
        channel.queueDeclare("my_expires_queue", false, false, false, args) ;

        // 释放资源
        channel.close();
        connection.close();
    }


    /**
     * 死信队列
     */
    @Test
    public void dlx() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        //
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        // 交换机申明
        channel.exchangeDeclare("exchange.dlx", "direct", true) ;
        channel.exchangeDeclare("exchange.normal", "fanout", true) ;
        // 队列申明并绑定交换机
        Map<String,Object> args = new HashMap<>() ;
        args.put("x-message-ttl", 10000) ;
        args.put("x-dead-letter-exchange", "exchange.dlx") ;
        args.put("x-dead-letter-routing-key", "routingKey") ;
        channel.queueDeclare("queue.normal", true, false, false, args) ;
        channel.queueBind("queue.normal","exchange.normal","") ;
        // 队列申明并绑定交换机
        channel.queueDeclare("queue.dlx", true, false, false, null) ;
        channel.queueBind("queue.dlx", "exchange.dlx", "routingKey") ;
        // 发送消息
        channel.basicPublish("exchange.normal",
                "rk",
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "dlx message content".getBytes());
        // 释放资源
        channel.close();
        connection.close();
    }
}
