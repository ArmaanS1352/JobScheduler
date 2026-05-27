package com.armaan.scheduler.controller;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.armaan.scheduler.model.Job;
import com.armaan.scheduler.service.JobService;


@RestController
@RequestMapping("jobs")
public class JobController {

    private final JobService jobService;

    @Autowired
    public JobController (JobService jobService) {
        this.jobService = jobService;
    }


    @PostMapping
    public Job createJob (@RequestBody Job job) {
        return jobService.createJob(job);
    }

    @GetMapping
    public List<Job> getAllJobs() {
        return jobService.getAllJobs();
    }

    @GetMapping("/{id}")
    public Job getJobById(@PathVariable UUID id) {
        return jobService.getJob(id);
    }
    
    
    @PatchMapping("/{id}/cancel")
    public Job cancelJob (@PathVariable UUID id) {
        return jobService.cancelJob(id);
    }
}
