package com.gopherflow.service;

import com.gopherflow.config.KafkaConfig;
import com.gopherflow.event.StageFailedEvent;
import com.gopherflow.event.StageReadyEvent;
import com.gopherflow.model.Stage;
import com.gopherflow.repository.StageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class StageExecutorService {

    private static final int MAX_RETRIES = 3;
    private static final String LOCK_PREFIX = "stage:lock:";
    private static final String RETRY_COUNT_PREFIX = "stage:retry:";

    private static final RedissonClient redissonClient = null;
    private static final RedisTemplate<String, String> redisTemplate = null;
    private static final KafkaTemplate<String, Object> kafkaTemplate = null;
    private static final StageRepository stageRepository = null;
    private static final WorkflowOrchestrationService orchestrationService = null;

    public static void executeWithLock(StageReadyEvent event) {
        String lockKey = LOCK_PREFIX + event.getStageId();
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // Try to acquire lock — wait 5s, hold for 30s
            boolean acquired = lock.tryLock(5, 30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Could not acquire lock for stage {} — another worker is running it",
                        event.getStageName());
                return;
            }

            log.info("Lock acquired for stage: {}", event.getStageName());
            executeStage(event);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for lock on stage: {}", event.getStageName());
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for stage: {}", event.getStageName());
            }
        }
    }

    private static void executeStage(StageReadyEvent event) {
        String retryKey = RETRY_COUNT_PREFIX + event.getStageId();
        String retryVal = redisTemplate.opsForValue().get(retryKey);
        int attempt = retryVal == null ? 1 : Integer.parseInt(retryVal) + 1;

        try {
            // Mark stage as RUNNING
            stageRepository.findById(event.getStageId()).ifPresent(stage -> {
                stage.setStatus(Stage.StageStatus.RUNNING);
                stageRepository.save(stage);
            });

            // Simulate execution (Phase 7: Rust worker takes over)
            log.info("Executing stage: {} (attempt {})", event.getStageName(), attempt);
            Thread.sleep(500);

            // Success — clear retry counter and notify orchestrator
            redisTemplate.delete(retryKey);
            log.info("Stage succeeded: {}", event.getStageName());
            orchestrationService.handleStageCompletion(
                    event.getWorkflowId(), event.getStageId());

        } catch (Exception e) {
            log.error("Stage failed: {} — attempt {}/{}", event.getStageName(), attempt, MAX_RETRIES);
            redisTemplate.opsForValue().set(retryKey, String.valueOf(attempt));

            if (attempt < MAX_RETRIES) {
                // Re-queue for retry
                log.info("Retrying stage: {}", event.getStageName());
                kafkaTemplate.send(KafkaConfig.STAGE_READY_TOPIC,
                        event.getStageId().toString(), event);
            } else {
                // Send to DLQ
                log.error("Stage {} exhausted retries — sending to DLQ", event.getStageName());
                redisTemplate.delete(retryKey);
                StageFailedEvent failedEvent = StageFailedEvent.builder()
                        .workflowId(event.getWorkflowId())
                        .stageId(event.getStageId())
                        .stageName(event.getStageName())
                        .errorMessage(e.getMessage())
                        .attemptNumber(attempt)
                        .build();
                kafkaTemplate.send(KafkaConfig.STAGE_DLQ_TOPIC,
                        event.getStageId().toString(), failedEvent);

                // Mark stage as FAILED
                stageRepository.findById(event.getStageId()).ifPresent(stage -> {
                    stage.setStatus(Stage.StageStatus.FAILED);
                    stageRepository.save(stage);
                });
            }
        }
    }
}