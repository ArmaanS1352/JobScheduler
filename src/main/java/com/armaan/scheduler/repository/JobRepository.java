package com.armaan.scheduler.repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.armaan.scheduler.model.Job;
import com.armaan.scheduler.model.JobStatus;

public interface JobRepository extends JpaRepository<Job, UUID>{
    public List<Job> findByScheduledTimeBeforeAndStatus(Instant time, JobStatus status);
}