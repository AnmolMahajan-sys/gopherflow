package com.gopherflow.dto;

import lombok.Data;

import java.util.List;
import java.util.UUID;

@Data
public class StageRequest {
    private String name;
    private List<UUID> dependsOn;
    private String payload;
}
