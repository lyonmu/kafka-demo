package space.muqingcloud;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MySimpleConsumer {
    private final static String TOPIC_NAME = "my-test-topic";
    private final static String SERVERS = "192.168.0.103:9092,192.168.0.103:9093,192.168.0.103:9094";
    private final static String CONSUMER_GROUP_NAME = "my-test-group";

    public static void main(String[] args) {

        Properties prop = new Properties();
        // 设置集群地址
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        // 设置消费组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        // 设置消息的key序列化
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置消息的value序列化
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 根据参数构建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        // 订阅指定的主题，默认消费所有分区的所有消息
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            /*
             * poll() API 是拉取消息的长轮询
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> System.out.println(new Gson().toJson(record)));
        }
    }
}
