package com.gopherflow.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class StageResponse {
    private UUID stageId;
    private String name;
    private String status;
}
