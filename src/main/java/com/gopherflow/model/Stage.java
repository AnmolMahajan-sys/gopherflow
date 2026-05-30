package com.gopherflow.model;


import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;


@Entity
@Table(name="stages")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class Stage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="workflow_id",nullable = false)
    private Workflow workflow;

    @Column(nullable=false)
    private String name;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(nullable=false, columnDefinition = "stage_status")
    private StageStatus status = StageStatus.PENDING;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name="depends_on", columnDefinition = "uuid[]")
    private UUID[] dependsOn;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String payload;

    @Builder.Default
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt=LocalDateTime.now();

    @Builder.Default
    @Column(name="updated_at",nullable = false)
    private LocalDateTime updatedAt=LocalDateTime.now();

    public enum StageStatus {
        PENDING,READY, RUNNING,COMPLETED,FAILED
    }
}
