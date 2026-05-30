package com.gopherflow.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.gopherflow.model.Workflow;
import java.util.UUID;

public interface WorkflowRepository extends JpaRepository<Workflow, UUID> {
}
