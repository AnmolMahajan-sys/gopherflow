package com.gopherflow.kafka;

import com.gopherflow.config.KafkaConfig;
import com.gopherflow.event.StageReadyEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StageEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishStageReady(StageReadyEvent event) {
        kafkaTemplate.send(KafkaConfig.STAGE_READY_TOPIC, event.getStageId().toString(), event);
        log.info("Published StageReadyEvent for stage: {}", event.getStageName());
    }
}
