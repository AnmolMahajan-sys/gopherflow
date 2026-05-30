package com.gopherflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    public static final String STAGE_READY_TOPIC="stage-ready";
    public static final String STAGE_COMPLETED_TOPIC="stage-completed";
    public static final String STAGE_FAILED_TOPIC="stage-failed";
    public static final String STAGE_DLQ_TOPIC = "stage-dead-letter";

    @Bean
    public NewTopic stageDlqTopic() {
        return TopicBuilder.name(STAGE_DLQ_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic stageReadyTopic() {
        return TopicBuilder.name(STAGE_READY_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic stageCompletedTopic() {
        return TopicBuilder.name(STAGE_COMPLETED_TOPIC).partitions(3).replicas(1).build();

    }

    @Bean
    public NewTopic stageFailedTopic() {
        return TopicBuilder.name(STAGE_FAILED_TOPIC).partitions(3).replicas(1).build();
    }
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public KafkaTemplate<String, Object> objectKafkaTemplate(){
        Map<String,Object> props=kafkaProperties.buildProducerProperties(null);
        ProducerFactory<String,Object> factory=new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}
