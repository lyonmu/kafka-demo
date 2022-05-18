package space.muqingcloud.controller;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class consumer {

    // 监听指定主题，并对每一条消息进行消费
    @KafkaListener(topics = "my-test-topic")
    public void listenGroup(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info(new Gson().toJson(record));
        //手动提交offset
        ack.acknowledge();
    }

    // 复杂消费：设置消费组、多topic、指定分区、指定偏移量消费及设置消费者个数
    @KafkaListener(
            groupId = "testGroup", // 消费组ID
            topicPartitions = { // 消费多个消费主题
                    @TopicPartition(topic = "topic1", partitions = {"0", "1"}), // 消费主题为：topic1的主题，消费0、1分区
                    @TopicPartition(
                            topic = "topic2",partitions = "0", // 消费主题为：topic2的主题，消费0分区
                            partitionOffsets = @PartitionOffset(
                                    partition = "1",initialOffset = "100" // 消费主题为：topic2的主题，从偏移量为100开始消费1分区
                            )
                    )
            },concurrency = "3" //concurrency就是同组下的消费者个数，就是并发消费数，建议小于等于分区总数
    )
    public void listenGroupPro(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String value = record.value();
        System.out.println("==============");
        System.out.println(value);
        System.out.println(record);
        System.out.println("==============");
        //手动提交offset
        ack.acknowledge();
    }
}
