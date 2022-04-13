package space.muqingcloud;

import cn.hutool.core.util.IdUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
        // 根据参数生成生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 生成一条消息对象
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, 0, IdUtil.simpleUUID(), "测试异步发送消息的value");
        // 异步发送消息,通过回调函数对异步结果进行处理
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("发送消息失败：" + exception.getStackTrace());
            }
            if (metadata != null) {
                System.out.println("异步方式发送消息结果：" + "topic:" + metadata.topic() + "\tpartition:" + metadata.partition() + "\toffset:" + metadata.offset());
            }
        });

        Thread.sleep(10000);
    }
}
