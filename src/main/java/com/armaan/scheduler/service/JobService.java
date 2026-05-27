package com.armaan.scheduler.service;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.armaan.scheduler.model.Job;
import com.armaan.scheduler.model.JobStatus;
import com.armaan.scheduler.model.JobType;
import com.armaan.scheduler.repository.JobRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class JobService {
    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper = new ObjectMapper(); //JSON parsing to handle http calls
    private final Map<JobType, ThrowingConsumer<Job>> registry = new EnumMap<>(JobType.class); //maps a throwingconsumer of type job to each entry in the JobType enum
    private final ExecutorService executor = Executors.newCachedThreadPool(); //runs jobs on a timeout thread
    
    @Autowired
    public JobService (JobRepository jobRepository) {
        this.jobRepository = jobRepository;
        registerDefaultHandlers();
    }



    public void execute(Job job) {
        //get handler of type JobType if exists, else return immediately
        ThrowingConsumer<Job> handler = registry.get(job.getJobType());
        if (handler == null) {
            markFailed(job, "No handler for job of type: " + job.getJobType());
            return;
        }

        markRunning(job);
        //submit handler to executor instead of just running now, avoids blocking main thread
        Future<?> f = executor.submit(() -> {
            try {
                handler.accept(job);
                markCompleted(job);
            } catch (Exception e) {
                markFailed(job, e.getMessage());
            }
        });

        //waits for the job to finish or enforces a timeout, shows correct message based on exception
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            markFailed(job, "Execution timed out.");
        } catch (ExecutionException ee) {
            markFailed(job, ee.getCause() != null ? ee.getCause().getMessage() : ee.getMessage());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            markFailed(job, "Execution interrupted.");
        }
    }

    //called in constructor, builds the registry map
    //each handler is a method reference matching ThrowingConsumer<Job>
    public void registerDefaultHandlers() {
        registry.put(JobType.PRINT, this::handlePrint);
        registry.put(JobType.HTTP_CALL, this::handleHttpCall);
    }

    private void handlePrint (Job job) {
        System.out.println("PRINT job payload: " + job.getPayload());
    }

    private void handleHttpCall (Job job) throws Exception {
        JsonNode node = objectMapper.readTree(job.getPayload());
        String url = node.get("url").asText();
        String method = node.path("method").asText();
        
        //simple client for http requests
        RestTemplate rt = new RestTemplate();

        if ("POST".equalsIgnoreCase(method)) {
            String body = node.path("body").asText();
            rt.postForEntity(url, body, String.class);
        } else {
            rt.getForEntity(url, String.class);
        }
    }

    private interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }






    public Job createJob (Job job) {
        job.setJobStatus(JobStatus.PENDING);
        job.setAttempts(0);
        job.setMaxAttempts(3);

        if (job.getScheduledTime() == null) 
            job.setScheduledTime(Instant.now());

        return jobRepository.save(job);
    }

    public Job cancelJob (UUID id){
        Job job = getJob(id);

        if (job.getJobStatus() == JobStatus.RUNNING ||
            job.getJobStatus() == JobStatus.COMPLETED)
            throw new RuntimeException("Can't cancel running or completed job.");

        job.setJobStatus(JobStatus.CANCELLED);
        return jobRepository.save(job);
    }

    public Job getJob (UUID id) {
        return jobRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Job not found."));
    }

    public List<Job> getDueJobs() {
        return jobRepository.findByScheduledTimeBeforeAndStatus(Instant.now(), JobStatus.PENDING);
    }

    public List<Job> getAllJobs() {
        return jobRepository.findAll();
    }





    public void markRunning (Job job) {
        job.setJobStatus(JobStatus.RUNNING);
        job.setStartedAt(Instant.now());
        jobRepository.save(job);
    }

    public void markCompleted (Job job) {
        job.setJobStatus(JobStatus.COMPLETED);
        job.setCompletedAt(Instant.now());
        jobRepository.save(job);
    }

    public void markFailed(Job job, String error) {
        job.setJobStatus(JobStatus.FAILED);
        job.setCompletedAt(Instant.now());
        job.setErrorMessage(error);
        jobRepository.save(job);
    }

    public void markRetrying(Job job) {
        job.setAttempts(job.getAttempts() + 1);
        job.setJobStatus(JobStatus.RETRYING);
        jobRepository.save(job);
    }
}
