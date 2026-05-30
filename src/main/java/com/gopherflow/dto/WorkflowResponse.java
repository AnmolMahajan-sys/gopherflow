package com.gopherflow.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class WorkflowResponse {
    private UUID workflowId;
    private String name;
    private String status;
    private List<StageResponse> stages;
}
