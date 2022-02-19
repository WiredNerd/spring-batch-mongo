package io.gihub.wirednerd.mongo.jobs;

import io.github.wirednerd.springbatch.mongo.configuration.MongodbBatchConfigurer;
import io.github.wirednerd.springbatch.mongo.repository.MongodbJobRepository;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class CreateUuidListTest {

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
    public void clearExistingExecutionData() {
        jobRepository = (MongodbJobRepository) batchConfigurer.getJobRepository();
        jobExplorer = batchConfigurer.getJobExplorer();

        mongoTemplate.remove(new Query(), jobRepository.getJobCollectionName());
    }

    @Test
    @Order(1)
    void runJob() throws Exception {
        jobLauncher.run(createUuidListJob, new JobParametersBuilder()
                .addDate("RunStart", new Date(System.currentTimeMillis()))
                .toJobParameters());

        assertEquals(10, listUuidWriter.getWrittenItems().size());

        assertEquals(1, jobExplorer.getJobInstanceCount(createUuidListJob.getName()));
    }

    @PreDestroy
    public void deleteCollections() {
        mongoTemplate.getCollectionNames().forEach(mongoTemplate::dropCollection);
    }
}