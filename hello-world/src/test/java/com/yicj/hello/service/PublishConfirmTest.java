package com.yicj.hello.service;

import com.rabbitmq.client.*;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class PublishConfirmTest extends BaseJunitClz {

    @Test
    public void confirmSelect() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        //
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        // 申明绑定队列
        channel.exchangeDeclare("exchange.confirm", "topic", true, false, null ) ;
        channel.queueDeclare("queue.confirm", true, false, false, null) ;
        channel.queueBind("queue.confirm", "exchange.confirm", "routingKey.confirm") ;
        //
        channel.confirmSelect() ;
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                log.info("handleAck =====> Ack, SeqNo: {}, multiple: {}", deliveryTag, multiple);
            }
            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                log.info("handleNack =====> Nack, SeqNo: {}, multiple: {}", deliveryTag, multiple);
            }
        });
        // 发送消息
        long nextPublishSeqNo = channel.getNextPublishSeqNo();
        // log.info("nextPublishSeqNo : {} ", nextPublishSeqNo);
        channel.basicPublish("exchange.confirm",
                "routingKey.confirm",
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "hello world".getBytes());

        Thread.sleep(3000);
        // 释放资源
        channel.close();
        connection.close();
    }
}
