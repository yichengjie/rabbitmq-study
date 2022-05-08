package com.yicj.hello.service;

import com.rabbitmq.client.*;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import java.io.IOException;
import java.util.concurrent.TimeoutException;


@Slf4j
public class PublishMandatoryTest extends BaseJunitClz {

    @Test
    public void handleReturn() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        // 建立连接
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        // 申明绑定资源
        channel.exchangeDeclare("exchange.return", "topic",true, false,null) ;
        channel.queueDeclare("queue.return", true, false, false, null) ;
        channel.queueBind("queue.return", "exchange.return", "routingKey.return") ;
        // 发送消息
        channel.basicPublish("exchange.return",
                "routingKey.return1",
                true,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "mandatory test".getBytes());
        channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            String msg = new String(body) ;
            log.info("======> msg : {}", msg);
        });
        Thread.sleep(3000);
        // 释放资源
        channel.close();
        connection.close();
    }

}
