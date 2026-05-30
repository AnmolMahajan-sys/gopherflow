package com.gopherflow.kafka;

import com.gopherflow.event.StageFailedEvent;
import com.gopherflow.event.StageReadyEvent;
import com.gopherflow.service.StageExecutorService;
import com.gopherflow.service.WorkflowOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StageEventConsumer {

    private final WorkflowOrchestrationService orchestrationService;

    @KafkaListener(topics = "stage-ready", groupId = "gopherflow-group")
    public void consumeStageReady(StageReadyEvent event) {
        log.info("Received stage: {}", event.getStageName());
        StageExecutorService.executeWithLock(event);
    }

    @KafkaListener(topics = "stage-dead-letter", groupId = "gopherflow-group")
    public void consumeDlq(StageFailedEvent event) {
        log.error("DLQ received failed stage: {} after {} attempts",
                event.getStageName(), event.getAttemptNumber());
    }

}