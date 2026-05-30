package com.gopherflow.controller;

import com.gopherflow.dto.WorkflowRequest;
import com.gopherflow.dto.WorkflowResponse;
import com.gopherflow.model.Stage;
import com.gopherflow.model.Workflow;
import com.gopherflow.repository.StageRepository;
import com.gopherflow.repository.WorkflowRepository;
import com.gopherflow.service.WorkflowOrchestrationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/workflows")
@RequiredArgsConstructor
public class WorkflowController {

    private final WorkflowOrchestrationService orchestrationService;
    private final WorkflowRepository workflowRepository;
    private final StageRepository stageRepository;

    @PostMapping
    public ResponseEntity<WorkflowResponse> registerWorkflow(
            @RequestBody WorkflowRequest request) {
        WorkflowResponse response = orchestrationService.registerWorkflow(request);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<?> getWorkflow(@PathVariable UUID workflowId) {
        return workflowRepository.findById(workflowId)
                .map(wf -> ResponseEntity.ok(wf))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{workflowId}/stages")
    public ResponseEntity<?> getStages(@PathVariable UUID workflowId) {
        return ResponseEntity.ok(stageRepository.findByWorkflowId(workflowId));
    }
}
