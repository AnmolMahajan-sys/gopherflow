package com.gopherflow.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DependencyGraphService {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String DEP_COUNTER_KEY = "stage:dep:counter:";
    private static final String STAGE_DEPS_KEY = "stage:deps:";

    public void initStageCounter(UUID stageId, int dependencyCount){
        String key = DEP_COUNTER_KEY + stageId;
        redisTemplate.opsForValue().set(key, String.valueOf(dependencyCount));
        log.info("Initialized dep counter for stage {}: {}",stageId,dependencyCount);
    }
    public Long decrementAndGet(UUID stageId){
        String key = DEP_COUNTER_KEY + stageId;
        Long remaining = redisTemplate.opsForValue().decrement(key);
        log.info("Decrement dep counter for stage {}: {}",stageId,remaining);
        return remaining;
    }
    public boolean isStageReady(UUID stageId){
        String key = DEP_COUNTER_KEY + stageId;
        String value = redisTemplate.opsForValue().get(key);
        return value == null||Long.parseLong(value)<=0;
    }
    public void clearStageCounter(UUID stageId){
        redisTemplate.delete(DEP_COUNTER_KEY + stageId);
        log.info("Cleared dep counter for stage {}",stageId);
    }
}
