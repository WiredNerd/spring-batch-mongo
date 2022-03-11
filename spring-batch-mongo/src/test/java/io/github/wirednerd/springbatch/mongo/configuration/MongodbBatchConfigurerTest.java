package io.github.wirednerd.springbatch.mongo.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wirednerd.springbatch.mongo.MongoDBContainerConfig;
import io.github.wirednerd.springbatch.mongo.explore.MongodbJobExplorer;
import io.github.wirednerd.springbatch.mongo.repository.MongodbJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.dao.DefaultExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MongodbBatchConfigurerTest extends MongoDBContainerConfig {

    private MongoTransactionManager mongoTransactionManager;

    private MongodbBatchConfigurer mongodbBatchConfigurer;

    private SyncTaskExecutor taskExecutor;
    private JobKeyGenerator<JobParameters> jobKeyGenerator;
    private Jackson2ExecutionContextStringSerializer executionContextSerializer;
    private Charset executionContextCharset;
    private ObjectMapper objectMapper;

    ArgumentCaptor<ObjectMapper> objectMapperCaptor = ArgumentCaptor.forClass(ObjectMapper.class);

    @BeforeEach
    void setupMongoTransactionManager() {
        mongoTransactionManager = new MongoTransactionManager(mongoDatabaseFactory);

        taskExecutor = new SyncTaskExecutor();
        jobKeyGenerator = new DefaultJobKeyGenerator();
        executionContextSerializer = Mockito.spy(new Jackson2ExecutionContextStringSerializer());
        executionContextCharset = StandardCharsets.ISO_8859_1;
        objectMapper = new ObjectMapper();
    }

    private void buildWithAllOptions() {
        mongodbBatchConfigurer = MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .jobCollectionName("jobs")
                .counterCollectionName("numbers")
                .taskExecutor(taskExecutor)
                .jobKeyGenerator(jobKeyGenerator)
                .executionContextSerializer(executionContextSerializer)
                .executionContextCharset(executionContextCharset)
                .objectMapper(objectMapper)
                .build();
    }

    @Test
    void constructor_nullMongoTemplate() {
        try {
            MongodbBatchConfigurer.builder()
                    .mongoTransactionManager(mongoTransactionManager)
                    .build();
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A MongoTemplate is required", e.getMessage());
        }
    }

    @Test
    void constructor_nullMongoTransactionManager() {
        try {
            MongodbBatchConfigurer.builder()
                    .mongoTemplate(mongoTemplate)
                    .build();
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A MongoTransactionManager is required", e.getMessage());
        }
    }

    @Test
    void constructor_blankJobCollection() {
        try {
            MongodbBatchConfigurer.builder()
                    .mongoTemplate(mongoTemplate)
                    .mongoTransactionManager(mongoTransactionManager)
                    .jobCollectionName("")
                    .build();
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Job Collection Name must not be null or blank", e.getMessage());
        }
    }

    @Test
    void constructor_blankCounterCollection() {
        try {
            MongodbBatchConfigurer.builder()
                    .mongoTemplate(mongoTemplate)
                    .mongoTransactionManager(mongoTransactionManager)
                    .counterCollectionName("")
                    .build();
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Counter Collection Name must not be null or blank", e.getMessage());
        }
    }

    @Test
    void constructor_ensureIndexes() {
        buildWithAllOptions();

        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();
        assertEquals(5, jobIndexes.size());
        assertEquals("_id_", jobIndexes.get(0).getName());
    }

    @Test
    void constructor_ensureIndexes_jobInstance_jobExecution_unique() {
        buildWithAllOptions();

        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();

        var jobIndex = jobIndexes.get(1);
        assertEquals("jobInstance_jobExecution_unique", jobIndex.getName());
        var indexFields = jobIndex.getIndexFields();
        assertEquals(3, indexFields.size());
        assertEquals(JOB_NAME, indexFields.get(0).getKey());
        assertEquals(JOB_KEY, indexFields.get(1).getKey());
        assertEquals(JOB_EXECUTION_ID, indexFields.get(2).getKey());
        assertTrue(jobIndex.isUnique());
    }

    @Test
    void constructor_ensureIndexes_jobExecutionId_unique() {
        buildWithAllOptions();

        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();

        var jobIndex = jobIndexes.get(2);
        assertEquals("jobExecutionId_unique", jobIndex.getName());
        var indexFields = jobIndex.getIndexFields();
        assertEquals(1, indexFields.size());
        assertEquals(JOB_EXECUTION_ID, indexFields.get(0).getKey());
        assertTrue(jobIndex.isUnique());
    }

    @Test
    void constructor_ensureIndexes_jobInstanceId() {
        buildWithAllOptions();

        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();

        var jobIndex = jobIndexes.get(3);
        assertEquals("jobInstanceId", jobIndex.getName());
        var indexFields = jobIndex.getIndexFields();
        assertEquals(1, indexFields.size());
        assertEquals(JOB_INSTANCE_ID, indexFields.get(0).getKey());
        assertFalse(jobIndex.isUnique());
    }

    @Test
    void constructor_ensureIndexes_jobName_jobInstanceId() {
        buildWithAllOptions();

        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();

        var jobIndex = jobIndexes.get(4);
        assertEquals("jobName_jobInstanceId", jobIndex.getName());
        var indexFields = jobIndex.getIndexFields();
        assertEquals(2, indexFields.size());
        assertEquals(JOB_NAME, indexFields.get(0).getKey());
        assertEquals(JOB_INSTANCE_ID, indexFields.get(1).getKey());
        assertFalse(jobIndex.isUnique());
    }

    @Test
    void getJobRepository() {
        buildWithAllOptions();

        assertTrue(mongodbBatchConfigurer.getJobRepository() instanceof MongodbJobRepository);
        MongodbJobRepository repository = ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository());
        assertSame(mongoTemplate, repository.getMongoTemplate());
        assertEquals("jobs", repository.getJobCollectionName());
        assertEquals("numbers", repository.getCounterCollectionName());
    }

    @Test
    void getTransactionManager() {
        buildWithAllOptions();

        assertSame(mongoTransactionManager, mongodbBatchConfigurer.getTransactionManager());
    }

    @Test
    void getJobLauncher() {
        buildWithAllOptions();

        assertTrue(mongodbBatchConfigurer.getJobLauncher() instanceof SimpleJobLauncher);
        assertSame(taskExecutor, ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "taskExecutor"));
        assertSame(mongodbBatchConfigurer.getJobRepository(), ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "jobRepository"));
    }

    @Test
    void getJobExplorer() {
        buildWithAllOptions();

        assertTrue(mongodbBatchConfigurer.getJobExplorer() instanceof MongodbJobExplorer);
        MongodbJobExplorer explorer = ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer());
        assertSame(mongoTemplate, explorer.getMongoTemplate());
        assertEquals("jobs", explorer.getJobCollectionName());
    }

    @Test
    void jobKeyGenerator() {
        buildWithAllOptions();

        assertSame(jobKeyGenerator, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getJobKeyGenerator());
        assertSame(jobKeyGenerator, ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer()).getJobExecutionDocumentMapper().getJobKeyGenerator());
    }

    @Test
    void executionContextSerializer() {
        buildWithAllOptions();

        assertSame(executionContextSerializer, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getExecutionContextSerializer());
        assertSame(executionContextSerializer, ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer()).getJobExecutionDocumentMapper().getExecutionContextSerializer());
    }

    @Test
    void objectMapper() {
        buildWithAllOptions();

        verify(executionContextSerializer).setObjectMapper(objectMapperCaptor.capture());
        assertSame(objectMapper, objectMapperCaptor.getValue());
    }

    @Test
    void objectMapper_notJackson2ExecutionContextStringSerializer() {
        mongodbBatchConfigurer = MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .executionContextSerializer(new DefaultExecutionContextSerializer())
                .objectMapper(objectMapper)
                .build();

        verify(executionContextSerializer, times(0)).setObjectMapper(Mockito.any());
    }

    @Test
    void executionContextCharset() {
        buildWithAllOptions();

        assertSame(executionContextCharset, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getExecutionContextCharset());
        assertSame(executionContextCharset, ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer()).getJobExecutionDocumentMapper().getExecutionContextCharset());
    }

    @Test
    void defaults() {
        mongodbBatchConfigurer = MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .build();

        assertTrue(mongodbBatchConfigurer.getJobRepository() instanceof MongodbJobRepository);
        MongodbJobRepository repository = ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository());
        assertSame(mongoTemplate, repository.getMongoTemplate());
        assertEquals("jobExecutions", repository.getJobCollectionName());
        assertEquals("counters", repository.getCounterCollectionName());

        assertTrue(mongodbBatchConfigurer.getJobLauncher() instanceof SimpleJobLauncher);
        assertTrue(ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "taskExecutor") instanceof SyncTaskExecutor);

        assertInstanceOf(DefaultJobKeyGenerator.class, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getJobKeyGenerator());
        assertInstanceOf(DefaultJobKeyGenerator.class, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getJobKeyGenerator());

        assertInstanceOf(Jackson2ExecutionContextStringSerializer.class, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository())
                .getJobExecutionDocumentMapper().getExecutionContextSerializer());
        assertInstanceOf(Jackson2ExecutionContextStringSerializer.class, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository())
                .getJobExecutionDocumentMapper().getExecutionContextSerializer());

        verify(executionContextSerializer, times(0)).setObjectMapper(Mockito.any());

        assertSame(StandardCharsets.UTF_8, ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository()).getJobExecutionDocumentMapper().getExecutionContextCharset());
        assertSame(StandardCharsets.UTF_8, ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer()).getJobExecutionDocumentMapper().getExecutionContextCharset());
    }
}