package space.muqingcloud.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;

@RestController
public class MessageController {

    private final static String TOPIC_NAME = "my-test-topic";

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/send/{id}")
    public String send(@PathVariable("id") Long id) {
        SendResult<String, String> result = null;

        try {
            result = kafkaTemplate.send(TOPIC_NAME, 0, "key:" + id, "this is a msg:" + id).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return "send success";
    }
}
