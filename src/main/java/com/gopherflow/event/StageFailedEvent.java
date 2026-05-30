package com.gopherflow.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StageFailedEvent {
    private UUID workflowId;
    private UUID stageId;
    private String stageName;
    private String errorMessage;
    private int attemptNumber;
}
