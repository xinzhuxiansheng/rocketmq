package com.yzhou.example.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

public class Producer01 {

    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("yzhou_producer01");
        // 设置NameServer地址
        producer.setNamesrvAddr("192.168.199.177:9876");
        // 增加超时时间
        producer.setSendMsgTimeout(60000);
        // 启动producer
        producer.start();

        int messageCount = 3;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);

        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message("yzhoutp01",
                    "TagA",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 异步发送消息, 发送结果通过callback返回给客户端
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }
}
