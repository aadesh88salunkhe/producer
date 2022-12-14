package com.ignite.producer.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Producer {

    @Value("${topic.name.hashtaglabel}")
    private String hashTagLabel;

    @Value("${topic.name.hashtagtweets}")
    private String hashTagTweets;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendHashTag(String message){
        kafkaTemplate.send(hashTagLabel, message);
        log.info("Payload enviado: {}" ,message);

    }

    public void sendHashTagTweets(String message){
        kafkaTemplate.send(hashTagTweets, message);
        log.info("Payload enviado: {}" ,message);

    }
}
