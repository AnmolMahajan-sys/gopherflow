package com.gopherflow.repository;

import com.gopherflow.model.Stage;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface StageRepository extends JpaRepository<Stage, UUID> {
    List<Stage> findByWorkflowId(UUID workflowId);
    List<Stage> findByWorkflowIdAndStatus(UUID workflowId, Stage.StageStatus status);
}
