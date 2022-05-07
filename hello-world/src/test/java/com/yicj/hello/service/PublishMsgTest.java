package com.yicj.hello.service;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

@Slf4j
public class PublishMsgTest extends BaseJunitClz {

    private final static String QUEUE_NAME = "my_queue" ; // 队列名称
    private final static String EXCHANGE_NAME = "my_exchange" ; // 要使用的exchange的名称
    private final static String EXCHANGE_TYPE = "topic" ; // 要使用的exchange的类型
    private final static String EXCHANGE_ROUTING_KEY = "my_routing_key.#" ; // exchange使用的routing-key


    //创建队列，发送消息
    @Test
    public void publishMsg() throws Exception {
        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201"); // 设置rabbitmq-server的地址
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/"); // 使用的虚拟主机
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        // 由链接工厂创建连接
        Connection connection = connectionFactory.newConnection();
        // 通过连接创建通道
        Channel channel = connection.createChannel();
        // 通过信道申明一个exchange，若已存在则直接使用，不存在自动创建
        // 参数：name、type 、是否支持持久化、此交换机没有绑定一个queue时是否自动删除、是否只在rabbitmq内部使用此交换机
        // 此交换机的其他参数(map)
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false,false, null) ;
        // 通过信道申明一个queue，如果此队列已存在，则直接使用，如果不存在，则自动创建
        // 参数：name、是否支持持久化、是否是排他的、是否支持自动删除、其他参数（map）
        channel.queueDeclare(QUEUE_NAME, true, false, false, null) ;
        // 将queue绑定至某个exchange，一个exchange可以绑定多个queue
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, EXCHANGE_ROUTING_KEY) ;
        // 发送消息
        String msg = "hello " ; // 消息内容
        String routing_key = "my_routing_key.key1" ; // 发送消息使用routing-key
        // 消息是byte[]，可以传递所有类型（转换为byte[]）, 不局限与字符串
        channel.basicPublish(EXCHANGE_NAME, routing_key, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes(StandardCharsets.UTF_8));
        log.info("send message : {}", msg);
        // 关闭连接
        channel.close();
        connection.close();
    }

}
