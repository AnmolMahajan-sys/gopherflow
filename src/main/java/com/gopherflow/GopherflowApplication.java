package com.gopherflow;

import com.gopherflow.event.StageReadyEvent;
import com.gopherflow.kafka.StageEventProducer;
import com.gopherflow.service.DependencyGraphService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@SpringBootApplication
@RestController
public class GopherflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(GopherflowApplication.class, args);
	}
	@GetMapping("/health")
	public Map<String,String> health() {
		return Map.of("status", "UP","service","gopherflow");
	}

	@Autowired
	private StageEventProducer producer;

	@PostMapping("/test-kafka")
	public Map<String,String> testKafka() {
		producer.publishStageReady(StageReadyEvent.builder().workflowId(UUID.randomUUID()).stageId(UUID.randomUUID()).stageName("test-stage").payload("{}").build());
		return Map.of("status", "event published");
	}

	@Autowired
	private DependencyGraphService dependencyGraphService;

	@GetMapping("/test-redis")
	public Map<String,Object> testRedis() {
		UUID stageId = UUID.randomUUID();

		dependencyGraphService.initStageCounter(stageId,3);
		long after1=dependencyGraphService.decrementAndGet(stageId);
		long after2=dependencyGraphService.decrementAndGet(stageId);
		long after3=dependencyGraphService.decrementAndGet(stageId);

		boolean ready=dependencyGraphService.isStageReady(stageId);
		dependencyGraphService.clearStageCounter(stageId);

		return Map.of(
				"stageId",stageId,
				"afterDecrement1",after1,
				"afterDecrement2",after2,
				"afterDecrement3",after3,
				"stageReady",ready
		);
	}
}
