package com.gopherflow.dto;

import lombok.Data;

import java.util.List;

@Data
public class WorkflowRequest {
    private String name;
    private List<StageRequest> stages;
}
