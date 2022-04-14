package space.muqingcloud;

import cn.hutool.core.util.IdUtil;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MySimpleSynchronizationProducer {
    private final static String TOPIC_NAME = "my-test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 生成参数对象，
        Properties props = new Properties();
        // 并配置 kafka 集群地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.103:9092,192.168.0.103:9093,192.168.0.103:9094");
        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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
        // 把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 根据参数生成生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 生成一条消息对象
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, 1, IdUtil.simpleUUID(), "测试各种参数的同步发送消息的value");
        // 发送消息,得到消息发送的元数据
        RecordMetadata metadata = producer.send(producerRecord).get();
        // 打印得到的消息发送的元数据
        // System.out.println("同步方式发送消息结果：" + "topic:" + metadata.topic() + "\tpartition:" + metadata.partition() + "\toffset:" + metadata.offset());
        System.out.println(new Gson().toJson(metadata));
    }

}
