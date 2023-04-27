package com.yzhou.example.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer01 {

    public static void main(String[] args) throws MQClientException {
        // Step 1: 创建一个 DefaultMQPushConsumer 实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("yzhoutpgid01");

        // Step 2: 设置 NameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // Step 3: 设置订阅主题和标签
        consumer.subscribe("yzhoutp01", "*");

        // Step 4: 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                }
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // Step 5: 启动消费者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
