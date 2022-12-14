package com.ignite.producer.kafka.model;

import lombok.Data;

@Data
public class HashTagMessage {
    String hashTag;
    String tweet;
}
