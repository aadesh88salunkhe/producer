package com.ignite.producer.startup;

import com.google.gson.Gson;
import com.ignite.producer.kafka.model.HashTag;
import com.ignite.producer.kafka.model.HashTagMessage;
import com.ignite.producer.kafka.producer.Producer;
import com.ignite.producer.utils.CSVHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.List;

@Slf4j
@Component
public class ReadFileService {
    @Autowired
    Producer producer;

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        List<HashTag> hashTags = CSVHelper.csvToHashTag(getClass().getClassLoader().getResourceAsStream("Hashtags.csv"));
        producer.sendHashTag(new Gson().toJson(hashTags));
        List<HashTagMessage> hashTagsTweets = CSVHelper.csvToHashTagMessages(getClass().getClassLoader().getResourceAsStream("HashtagMessageData.csv"));
        producer.sendHashTagTweets(new Gson().toJson(hashTagsTweets));
        log.info("Hashtag data and tweets has been sent to kafka Topics! {} ");
    }
}
