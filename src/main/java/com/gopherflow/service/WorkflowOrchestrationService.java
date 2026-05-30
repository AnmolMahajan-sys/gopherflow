package com.gopherflow.service;

import com.gopherflow.dto.StageRequest;
import com.gopherflow.dto.WorkflowRequest;
import com.gopherflow.dto.WorkflowResponse;
import com.gopherflow.dto.StageResponse;
import com.gopherflow.event.StageReadyEvent;
import com.gopherflow.kafka.StageEventProducer;
import com.gopherflow.model.Stage;
import com.gopherflow.model.Workflow;
import com.gopherflow.repository.StageRepository;
import com.gopherflow.repository.WorkflowRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowOrchestrationService {

    private final WorkflowRepository workflowRepository;
    private final StageRepository stageRepository;
    private final DependencyGraphService dependencyGraphService;
    private final StageEventProducer stageEventProducer;

    @Transactional
    public WorkflowResponse registerWorkflow(WorkflowRequest request) {
        // 1. Save workflow to PostgreSQL
        Workflow workflow = Workflow.builder()
                .name(request.getName())
                .status(Workflow.WorkflowStatus.PENDING)
                .build();
        workflow = workflowRepository.save(workflow);
        log.info("Registered workflow: {} ({})", workflow.getName(), workflow.getId());

        // 2. Save all stages
        List<Stage> stages = new ArrayList<>();
        for (StageRequest sr : request.getStages()) {
            Stage stage = Stage.builder()
                    .workflow(workflow)
                    .name(sr.getName())
                    .status(Stage.StageStatus.PENDING)
                    .dependsOn(sr.getDependsOn() != null
                            ? sr.getDependsOn().toArray(new UUID[0])
                            : new UUID[0])
                    .payload(sr.getPayload())
                    .build();
            stages.add(stageRepository.save(stage));
        }

        // 3. Initialize Redis counters and fire ready stages
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflowRepository.save(workflow);

        for (Stage stage : stages) {
            int depCount = stage.getDependsOn() != null ? stage.getDependsOn().length : 0;
            dependencyGraphService.initStageCounter(stage.getId(), depCount);

            if (depCount == 0) {
                // No dependencies — fire immediately
                fireStage(stage);
            }
        }

        return buildResponse(workflow, stages);
    }

    @Transactional
    public void handleStageCompletion(UUID workflowId, UUID completedStageId) {
        log.info("Handling completion of stage: {}", completedStageId);

        // Mark the completed stage
        stageRepository.findById(completedStageId).ifPresent(stage -> {
            stage.setStatus(Stage.StageStatus.COMPLETED);
            stageRepository.save(stage);
        });

        // Find all stages in this workflow that depend on the completed stage
        List<Stage> allStages = stageRepository.findByWorkflowId(workflowId);

        for (Stage stage : allStages) {
            if (stage.getStatus() != Stage.StageStatus.PENDING) continue;
            if (stage.getDependsOn() == null) continue;

            for (UUID depId : stage.getDependsOn()) {
                if (depId.equals(completedStageId)) {
                    // Decrement this stage's counter
                    Long remaining = dependencyGraphService.decrementAndGet(stage.getId());
                    if (remaining != null && remaining <= 0) {
                        fireStage(stage);
                    }
                    break;
                }
            }
        }

        // Check if entire workflow is complete
        checkWorkflowCompletion(workflowId, allStages);
    }

    private void fireStage(Stage stage) {
        stage.setStatus(Stage.StageStatus.READY);
        stageRepository.save(stage);

        StageReadyEvent event = StageReadyEvent.builder()
                .workflowId(stage.getWorkflow().getId())
                .stageId(stage.getId())
                .stageName(stage.getName())
                .payload(stage.getPayload())
                .build();

        stageEventProducer.publishStageReady(event);
        log.info("Fired stage: {}", stage.getName());
    }

    private void checkWorkflowCompletion(UUID workflowId, List<Stage> stages) {
        boolean allDone = stages.stream()
                .allMatch(s -> s.getStatus() == Stage.StageStatus.COMPLETED);
        if (allDone) {
            workflowRepository.findById(workflowId).ifPresent(wf -> {
                wf.setStatus(Workflow.WorkflowStatus.COMPLETED);
                workflowRepository.save(wf);
                log.info("Workflow {} completed!", workflowId);
            });
        }
    }

    private WorkflowResponse buildResponse(Workflow workflow, List<Stage> stages) {
        List<StageResponse> stageResponses = stages.stream()
                .map(s -> StageResponse.builder()
                        .stageId(s.getId())
                        .name(s.getName())
                        .status(s.getStatus().name())
                        .build())
                .toList();

        return WorkflowResponse.builder()
                .workflowId(workflow.getId())
                .name(workflow.getName())
                .status(workflow.getStatus().name())
                .stages(stageResponses)
                .build();
    }
}
