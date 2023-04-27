package com.yzhou.example.producer;

import io.netty.channel.DefaultChannelId;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务写入
 */
public class ProducerTransaction03 {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // Step 1: 创建一个 TransactionMQProducer 实例
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer_group");
        // Step 2: 设置 NameServer 地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setSendMsgTimeout(60000);
        // Step 3: 注册事务监听器
        producer.setTransactionListener(new TransactionListener() {
            // yzhou 什么时候执行
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                // 在执行本地事务之前，将事务 ID 和状态插入到数据库中

                System.out.println("yzhou executeLocalTransaction");
                Integer.parseInt("abcd");
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
//                // TODO: 根据 msg.getTransactionId() 检查事务状态，具体根据业务需求实现
                System.out.println("yzhou checkLocalTransaction");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        DefaultChannelId.newInstance();
        // Step 4: 启动生产者
        producer.start();

        // Step 5: 构造一个 Message 实例
        Message message = new Message("yzhoutp03", "transaction_tag", "transaction_key", "Hello Transaction Message".getBytes());

        // Step 6: 调用 sendMessageInTransaction 方法发送事务消息
        SendResult sendResult = producer.sendMessageInTransaction(message, null);
        System.out.printf("Transaction Message Send Result: %s%n", sendResult);

        // 暂停，以便观察事务消息的发送和消费过程
        Thread.sleep(1000 * 10);

        // Step 7: 在适当的时候关闭生产者
        producer.shutdown();
    }
}
