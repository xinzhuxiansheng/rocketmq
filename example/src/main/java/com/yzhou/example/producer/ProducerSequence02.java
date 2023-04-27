package com.yzhou.example.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ProducerSequence02 {

    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("yzhou_producer01");
        // 设置NameServer地址
        producer.setNamesrvAddr("192.168.199.177:9876");
        // 增加超时时间
        producer.setSendMsgTimeout(60000);
        // 启动producer
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        String topic = "yzhoutp02";
        String tags = "TagA";
        String keys = "KEY";

        String [] sequenceStr = new String[]{"A","B","C","D"};

        int index = 0;
        while (true) {
            String body = "Hello RocketMQ " + index;
            Message msg = new Message(topic, tags, keys, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            int seqIndex = index % sequenceStr.length;

            int finalIndex = index;

            /*
                1. 自定义MessageQueueSelector，在select()处理传入index
                2. 若对消息的tag或者key或者 数据中某个字段做 hash，则可以不用传index，此时 arg的形参传null即可
             */
            producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    int hashCode = arg.toString().hashCode();
                    int index = hashCode % mqs.size();
                    return mqs.get(index);
                }
            }, sequenceStr[seqIndex], new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", finalIndex,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", finalIndex, e);
                    e.printStackTrace();
                }
            }); // i is the unique routing key, e.g., order ID
            index++;
            Thread.sleep(10000);
        }
    }
}
