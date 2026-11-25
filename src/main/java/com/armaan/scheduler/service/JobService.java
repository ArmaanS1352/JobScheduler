package com.armaan.scheduler.service;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
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
    private final Set<UUID> activeJobs = ConcurrentHashMap.newKeySet();
    private enum WorkerState {
        NOT_STARTED,
        RUNNING,
        FINISHED,
        CANCELLED_BEFORE_START
    }
    
    @Autowired
    public JobService (JobRepository jobRepository) {
        this.jobRepository = jobRepository;
        registerDefaultHandlers();
    }


/*
-attempt to add job id to set, if it fails then the job's claimed by another thread, return
-init workerSubmitted as false because executor hasnt yet successfully taken the job even though its a valid pending task id
-if executor fails to receive the job, the inner try catch throws exception to the outer catch/finally which will release the job id
-if executor receives the job, the inner finally releases the id and the outer finally has no effect whether or not it runs
-point of the workerSubmitted var is in case 3 where job is submitted to executor but the job outlives the f.get timer so cancellation is
requested in the timeoutexception catch, but the request does not necessarily force immediate end of the job, but the code will immediately
move on to the finally block regardless and release the jobId while its still working if not for the check

-case 1: executor.submit throws before accepting the task, the outer finally releases the job id
-case 2: task submitted but timeout/interruption occurs before worker starts lambda execution, outer catch statements release job id
    -the catch/finally of a submitted task is in its function, if function exec never reached, finally not able to release the job id
-case 3: worker starts and completes or throws exception while running, workers finally releases job id
-case 4: worker starts but doesnt stop immediately after cancel request made, workers finally releases id whenever it finishes
    - atomicref workerstate maintains the jobs claimed status until the job is actually done, preventing it from being picked up again
*/
    public void execute(Job job) {
        ThrowingConsumer<Job> handler = registry.get(job.getJobType());
        
        if (handler == null) {
            markFailed(job, "No handler for job of type: " + job.getJobType());
            return;
        }
        System.out.println("ATTEMPTING TO CLAIM: " + job.getUuid());
        if (!activeJobs.add(job.getUuid())) {
            System.out.println("JOB ALREADY ACTIVE: " + job.getUuid());
            return;
        }
        

        boolean workerSubmitted = false;
        AtomicReference<WorkerState> workerState = new AtomicReference<>(WorkerState.NOT_STARTED);

        try {
            markRunning(job);

            Future<?> f = executor.submit(() -> {

                if (!workerState.compareAndSet(WorkerState.NOT_STARTED, WorkerState.RUNNING))
                    return;

                try {
                    System.out.println("WORKER STARTED: " + job.getUuid());
                    handler.accept(job);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    System.out.println("WORKER FINISHED: " + job.getUuid());
                    workerState.set(WorkerState.FINISHED);
                    activeJobs.remove(job.getUuid());
                }

            });

            workerSubmitted = true;

            try {
                f.get(5, TimeUnit.SECONDS);
                markCompleted(job);
            } 
            catch (TimeoutException e) {
                f.cancel(true);

                if (workerState.compareAndSet(WorkerState.NOT_STARTED, WorkerState.CANCELLED_BEFORE_START))
                    activeJobs.remove(job.getUuid());

                handleFailed(job, "Execution timed out.");
            } 
            catch (ExecutionException e) {
                System.out.println("JOB EXEC FAILED " + e.getCause());
                handleFailed(job, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            } 
            catch (InterruptedException e) {
                f.cancel(true);

                if (workerState.compareAndSet(WorkerState.NOT_STARTED, WorkerState.CANCELLED_BEFORE_START))
                    activeJobs.remove(job.getUuid());

                Thread.currentThread().interrupt();
                handleFailed(job, "Execution interrupted.");
            } 

        } 
        finally {
            if (!workerSubmitted) {
                activeJobs.remove(job.getUuid());
            }
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
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(20000);
        RestTemplate rt = new RestTemplate(factory);

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
        return jobRepository.findByScheduledTimeBeforeAndStatusIn(
            Instant.now(), 
            List.of(JobStatus.PENDING, JobStatus.RETRYING)
        );
    }

    public List<Job> getAllJobs() {
        return jobRepository.findAll();
    }

    public List<Job> getJobsByStatus (JobStatus status) {
        return jobRepository.findByStatus(status);
    }

    public void markRunning (Job job) {
        job.setJobStatus(JobStatus.RUNNING);
        job.setStartedAt(Instant.now());
        job.setAttempts(job.getAttempts() + 1);
        jobRepository.save(job);
    }

    public void markCompleted (Job job) {
        job.setJobStatus(JobStatus.COMPLETED);
        job.setCompletedAt(Instant.now());
        job.setErrorMessage(null);
        jobRepository.save(job);
    }

    public void markFailed(Job job, String error) {
        job.setJobStatus(JobStatus.FAILED);
        job.setCompletedAt(Instant.now());
        job.setErrorMessage(error);
        jobRepository.save(job);
    }

    public void handleFailed (Job job, String error) {
        System.out.println("HANDLING FAILURE" + error);
        if (job.getAttempts() < job.getMaxAttempts())
            markRetrying(job, error);
        else
            markFailed(job, error);
    }

    public void markRetrying(Job job, String error) {
        System.out.println("JOB RETRYING " + job.getAttempts());
        long delaySeconds = 3;
        job.setScheduledTime(Instant.now().plusSeconds(delaySeconds));
        job.setJobStatus(JobStatus.RETRYING);
        job.setErrorMessage(error);
        jobRepository.save(job);
    }
}
