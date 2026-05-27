package com.armaan.scheduler.scheduler;

import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.armaan.scheduler.model.Job;
import com.armaan.scheduler.service.JobService;

@Service
public class JobRunner {
    private final JobService jobService;

    public JobRunner (JobService jobService) {
        this.jobService = jobService;
    }

    //every 5s for now
    @Scheduled(fixedRate = 5000)
    public void processJobs() {
        List<Job> jobs = jobService.getDueJobs();

        for (Job job : jobs) {
            jobService.execute(job);
        }
    }
}
