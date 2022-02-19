package io.gihub.wirednerd.mongo.jobs;

import io.github.wirednerd.springbatch.mongo.configuration.MongodbBatchConfigurer;
import io.github.wirednerd.springbatch.mongo.repository.MongodbJobRepository;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
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
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RunAndRerunJobTest {

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
    void runJobWithParameters() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParametersBuilder()
                .addDate("RunStart", new Date(System.currentTimeMillis()))
                .toJobParameters());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(1, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(1, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());
    }

    @Test
    @Order(2)
    void reRunJobWithUpdatedParameters() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParametersBuilder()
                .addDate("RunStart", new Date(System.currentTimeMillis()))
                .toJobParameters());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(2, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(1, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());
    }

    @Test
    @Order(3)
    void runJobWithoutParameters() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParameters());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(3, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(1, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());
    }

    @Test
    @Order(4)
    void reRunJobWithoutParameters() throws Exception {
        var jobExecution = jobLauncher.run(createUuidListJob, new JobParameters());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(3, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
        assertEquals(2, jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size());
    }
}