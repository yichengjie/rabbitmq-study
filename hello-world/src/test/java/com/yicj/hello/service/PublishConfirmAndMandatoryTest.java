package com.yicj.hello.service;

import com.rabbitmq.client.*;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Slf4j
public class PublishConfirmAndMandatoryTest extends BaseJunitClz {

    @Test
    public void handleConfirmReturn() throws IOException, TimeoutException, InterruptedException {
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
        channel.confirmSelect() ;
        //1. 消息发送确认，消息发送成功或者失败会回调对应的方法
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                log.info("confirm listener handleAck =====>  deliveryTag: {}, multiple: {}", deliveryTag, multiple);
            }
            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                log.info("confirm listener handleNack =====>  deliveryTag: {}, multiple: {}", deliveryTag, multiple);
            }
        });

        // 2. 返回回调，交换机无法根据自身的类型和路由健找到一个符合条件的队列，那么RabbitMQ会调用Basic.Return命令将消息返回给生产者
        channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            String msg = new String(body) ;
            log.info("return listener ======> msg : {}", msg);
        });

        channel.basicPublish("exchange.return",
                "routingKey.return1",
                true,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "mandatory test".getBytes());

        Thread.sleep(3000);
        // 释放资源
        channel.close();
        connection.close();
    }
}
