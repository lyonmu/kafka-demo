package space.muqingcloud;

import cn.hutool.core.util.IdUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MySimpleAsynchronousProducer {
    private final static String TOPIC_NAME = "my-test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 生成参数对象，
        Properties props = new Properties();
        // 并配置 kafka 集群地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.103:9092,192.168.0.103:9093,192.168.0.103:9094");
        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置ack参数
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // 设置重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 设置重试的时间间隔,单位毫秒
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        // 设置本地缓存区，如果设置了本地缓存区，发送的消息会先进入到本地缓冲区，提高消息发送的性能。默认为32MB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 设置每次从本地缓存区批量读取的数据大小，默认是16KB
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 设置发送每隔多少时间从缓存区读取16KB的数据进行发送，不满16KB也算16KB,单位是毫秒
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 根据参数生成生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        final CountDownLatch countDownLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            // 生成一条消息对象
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, 0, IdUtil.simpleUUID(), "测试异步发送消息的value" + i);
            // 异步发送消息,通过回调函数对异步结果进行处理
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("发送消息失败：" + exception.getStackTrace());
                }
                if (metadata != null) {
                    System.out.println("异步方式发送消息结果：" + "topic:" + metadata.topic() + "\tpartition:" + metadata.partition() + "\toffset:" + metadata.offset());
                }
                countDownLatch.countDown(); // 发送完消息后countDownLatch进行减一操作
            });
        }
        // 判断countDownLatch是不是0，如果不是0就阻塞当前线程5秒
        countDownLatch.await(5, TimeUnit.SECONDS);
        // 发送完毕关闭发送者
        producer.close();

    }
}
