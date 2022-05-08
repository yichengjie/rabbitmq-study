package com.yicj.hello.service;

import com.rabbitmq.client.*;
import com.yicj.hello.BaseJunitClz;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ConsumerMsgTest extends BaseJunitClz {

    private final static String QUEUE_NAME = "my_queue" ; // 队列名称
    private final static String EXCHANGE_NAME = "my_exchange" ; // 要使用的exchange的名称
    private final static String EXCHANGE_TYPE = "topic" ; // 要使用exchange的类型
    private final static String EXCHANGE_ROUTING_KEY = "my_routing_key.#" ; // exchange使用的routing-key

    @Test
    public void receive() throws IOException, TimeoutException, InterruptedException {
        // 创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost("192.168.99.201");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        // 由连接工厂创建连接
        Connection connection = connectionFactory.newConnection();
        // 通过连接创建信道
        Channel channel = connection.createChannel();
        // 通过信道申明一个exchange,若已存在则直接使用，不存在会自动创建
        // 参数： name、type、是否支持持久化、此交换机没有绑定一个queue时是否自动删除、是否只在rabbitmq内部使用此交换机、交换机其他参数（map）
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, false, null) ;
        // 通过信道申明一个queue，如果此队列已经存在，则直接使用，如果不存在，会自动创建
        // 参数：name、是否支持持久化、是否是排他的、是否支持自动删除、其他参数（map）
        channel.queueDeclare(QUEUE_NAME, true, false, false, null );
        // 将queue绑定至某个exchange，一个exchange可以绑定多个queue
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, EXCHANGE_ROUTING_KEY) ;
        // 创建消费者，指定要使用的channel, QueueingConsume类已经废弃，使用DefaultConsumer替代
        // 自动签收消息
        //DefaultConsumer consumer = new DefaultConsumer(channel){
        //    @Override
        //    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        //        String msg = new String(body) ;
        //        log.info("====> msg : {}", msg);
        //    }
        //} ;
        //// 监听指定的queue，会一直监听
        //// 参数： 要监听的queue、是否自动确认消息、使用的Consumer
        //channel.basicConsume(QUEUE_NAME, true, consumer) ;
        // 手动签收消息
        channel.basicQos(16);
        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body) ;
                log.info("====> msg: {}", msg);
                log.info("properties : {}", properties);
                // 处理完成后，应答签收
                channel.basicAck(envelope.getDeliveryTag(), false);
                // 拒收
                //channel.basicReject(envelope.getDeliveryTag(), true);
            }
        } ;
        // 监听指定的queue会一直监听
        // 参数：要监听的queue、是否自动确认消息、使用的Consumer
        channel.basicConsume(QUEUE_NAME, false, consumer) ;
        Thread.sleep(1000);
        // 关闭连接
        channel.close();
        connection.close();
    }
}
