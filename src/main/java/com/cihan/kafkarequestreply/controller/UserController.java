package com.cihan.kafkarequestreply.controller;

import com.cihan.kafkarequestreply.model.User;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class UserController {
    @Autowired
    ReplyingKafkaTemplate<String, User,User> kafkaTemplate;


    String requestTopic = "USER_TOPIC";


    String requestReplyTopic="USER_TOPIC_REPLY";

    @ResponseBody
    @PostMapping(value="/user",produces= MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
    public User user(@RequestBody User request) throws InterruptedException, ExecutionException {
        ProducerRecord<String, User> record = new ProducerRecord<String, User>(requestTopic, request);
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        RequestReplyFuture<String, User, User> sendAndReceive = kafkaTemplate.sendAndReceive(record);
        return sendAndReceive.get().value();
    }
}
