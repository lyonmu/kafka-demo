package space.muqingcloud;

import cn.hutool.core.util.IdUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
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
        // 把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 根据参数生成生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 生成一条消息对象
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, 1, IdUtil.simpleUUID(), "测试发送消息的value");
        // 发送消息,得到消息发送的元数据
        RecordMetadata metadata = producer.send(producerRecord).get();
        // 打印得到的消息发送的元数据
        System.out.println("同步方式发送消息结果：" + "topic:" + metadata.topic() + "\tpartition:" + metadata.partition() + "\toffset:" + metadata.offset());
    }
}
