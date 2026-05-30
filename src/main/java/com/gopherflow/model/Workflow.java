package com.gopherflow.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name="workflows")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Workflow {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable =false)
    private String name;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(nullable = false, columnDefinition = "workflow_status")
    private WorkflowStatus status=WorkflowStatus.PENDING;

    @Builder.Default
    @Column(name="created_at",nullable = false,updatable = false)
    private LocalDateTime createdAt=LocalDateTime.now();

    @Builder.Default
    @Column(name="updated_at",nullable = false)
    private LocalDateTime updatedAt=LocalDateTime.now();

    public enum WorkflowStatus {
        PENDING,RUNNING,FINISHED,COMPLETED
    }
}
