package com.armaan.scheduler.model;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;


@Entity
@Table (name = "jobs")
public class Job {
    @Id
    @GeneratedValue
    private UUID id;
    private String name;

    @Column(columnDefinition="TEXT")
    private String payload;
    private String errorMessage;

    private Instant scheduledTime;
    private Instant startedAt;
    private Instant completedAt;

    @Enumerated(EnumType.STRING)
    private JobStatus status;
    @Enumerated(EnumType.STRING)
    private JobType jobType;

    private int attempts;
    private int maxAttempts;

    public Job () {
        name = "";
        payload = "";
        scheduledTime = Instant.now();
        status = JobStatus.PENDING;
        attempts = 0;
        maxAttempts = 3;
    }

    public Job (String name, String payload, Instant scheduledTime, JobStatus status) {
        this.name = name;
        this.payload = payload;
        this.scheduledTime = scheduledTime;
        this.status = status;
        this.attempts = 0;
        this.maxAttempts = 3;
    }

    public UUID getUuid() {return id;}

    public String getName() {return name; }
    public void setName (String name) {this.name = name; }

    public String getPayload() {return payload;}
    public void setPayload (String payload) {this.payload = payload; }

    public Instant getScheduledTime() {return scheduledTime;}
    public void setScheduledTime (Instant scheduledTime) {this.scheduledTime = scheduledTime;}

    public JobStatus getJobStatus() {return status;}
    public void setJobStatus (JobStatus status) {this.status = status;}

    public int getAttempts() {return attempts;}
    public void setAttempts (int attempts) { this.attempts = attempts;}

    public int getMaxAttempts() {return maxAttempts;}
    public void setMaxAttempts (int maxAttempts) {this.maxAttempts = maxAttempts;}

    public String getErrorMessage() {return errorMessage;}
    public void setErrorMessage (String errorMessage) {this.errorMessage = errorMessage;}

    public Instant getStartedAt() {return startedAt;}
    public void setStartedAt (Instant startedAt) {this.startedAt = startedAt;}

    public Instant getCompltetedAt() {return completedAt;}
    public void setCompletedAt (Instant completedAt) {this.completedAt = completedAt;}

    public JobType getJobType() {return jobType;}
    public void setJobType(JobType jobType) {this.jobType = jobType;}
}
