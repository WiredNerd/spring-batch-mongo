package io.gihub.wirednerd.mongo.jobs;

import io.github.wirednerd.springbatch.mongo.configuration.MongodbBatchConfigurer;
import io.github.wirednerd.springbatch.mongo.repository.MongodbJobRepository;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;

import javax.annotation.PostConstruct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DirtiesContext
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RestartJobTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongodbBatchConfigurer batchConfigurer;

    private MongodbJobRepository jobRepository;
    private JobExplorer jobExplorer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job createUuidListJob;

    @Autowired
    private ListItemWriter<Object> listUuidWriter;

    @PostConstruct
    public void setupObjects() {
        jobRepository = (MongodbJobRepository) batchConfigurer.getJobRepository();
        jobExplorer = batchConfigurer.getJobExplorer();
    }

    @Test
    @Order(0)
    public void clearExistingExecutionData() {
        mongoTemplate.remove(new Query(), jobRepository.getJobCollectionName());

        assertEquals(0, mongoTemplate.count(new Query(), jobRepository.getJobCollectionName()));
    }

    @Test
    @Order(1)
    void jobFails() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParametersBuilder()
                .addLong("limit", 40L)
                .addLong("errorOn", 13L, false)
                .toJobParameters());

        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
        assertEquals("FAILED", jobExecution.getExitStatus().getExitCode());
        assertTrue(jobExecution.getExitStatus().getExitDescription().contains("RuntimeException: Failed to process row 13"),
                jobExecution.getExitStatus().getExitDescription());
        assertEquals(1, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(1, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());

        var step = jobExecution.getStepExecutions().iterator().next();
        assertEquals(BatchStatus.FAILED, step.getStatus());
        assertEquals(13, step.getReadCount());
        assertEquals(10, step.getWriteCount());
        assertEquals(10, step.getExecutionContext().getLong("processed"));
    }

    @Test
    @Order(2)
    void jobCompletes() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParametersBuilder()
                .addLong("limit", 40L)
                .toJobParameters());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(1, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(2, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());

        var step = jobExecution.getStepExecutions().iterator().next();
        assertEquals(BatchStatus.COMPLETED, step.getStatus());
        assertEquals(30, step.getReadCount());
        assertEquals(30, step.getWriteCount());
        assertEquals(40, step.getExecutionContext().getLong("processed"));
    }
}