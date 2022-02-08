package wirednerd.springbatch.mongo.configuration;

import com.mongodb.MongoClientOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import wirednerd.springbatch.mongo.explore.MongodbJobExplorer;
import wirednerd.springbatch.mongo.repository.MongodbJobRepository;

import static org.junit.jupiter.api.Assertions.*;
import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

@SpringBootTest
@MongoUnitTest
class MongodbBatchConfigurerTest {

    private MongoClientOptions a;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoDatabaseFactory mongoDatabaseFactory;

    private MongoTransactionManager mongoTransactionManager;

    private MongodbBatchConfigurer mongodbBatchConfigurer;

    private SyncTaskExecutor taskExecutor;

    @BeforeEach
    void setupMongoTransactionManager() {
        mongoTransactionManager = new MongoTransactionManager(mongoDatabaseFactory);

        taskExecutor = new SyncTaskExecutor();

        mongodbBatchConfigurer = MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .jobCollectionName("jobs")
                .counterCollectionName("numbers")
                .taskExecutor(taskExecutor)
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
        var jobIndexes = mongoTemplate.indexOps("jobs").getIndexInfo();
        assertEquals(5, jobIndexes.size());
        assertEquals("_id_", jobIndexes.get(0).getName());
    }

    @Test
    void constructor_ensureIndexes_jobInstance_jobExecution_unique() {
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
        assertTrue(mongodbBatchConfigurer.getJobRepository() instanceof MongodbJobRepository);
        MongodbJobRepository repository = ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository());
        assertTrue(mongoTemplate == repository.getMongoTemplate());
        assertEquals("jobs", repository.getJobCollectionName());
        assertEquals("numbers", repository.getCounterCollectionName());
    }

    @Test
    void getTransactionManager() {
        assertTrue(mongoTransactionManager == mongodbBatchConfigurer.getTransactionManager());
    }

    @Test
    void getJobLauncher() {
        assertTrue(mongodbBatchConfigurer.getJobLauncher() instanceof SimpleJobLauncher);
        assertTrue(taskExecutor == ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "taskExecutor"));
        assertTrue(mongodbBatchConfigurer.getJobRepository() == ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "jobRepository"));
    }

    @Test
    void getJobExplorer() {
        assertTrue(mongodbBatchConfigurer.getJobExplorer() instanceof MongodbJobExplorer);
        MongodbJobExplorer explorer = ((MongodbJobExplorer) mongodbBatchConfigurer.getJobExplorer());
        assertTrue(mongoTemplate == explorer.getMongoTemplate());
        assertEquals("jobs", explorer.getJobCollectionName());
    }

    @Test
    void defaults() {
        mongodbBatchConfigurer = MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .build();

        assertTrue(mongodbBatchConfigurer.getJobRepository() instanceof MongodbJobRepository);
        MongodbJobRepository repository = ((MongodbJobRepository) mongodbBatchConfigurer.getJobRepository());
        assertTrue(mongoTemplate == repository.getMongoTemplate());
        assertEquals("jobExecutions", repository.getJobCollectionName());
        assertEquals("counters", repository.getCounterCollectionName());

        assertTrue(mongodbBatchConfigurer.getJobLauncher() instanceof SimpleJobLauncher);
        assertTrue(ReflectionTestUtils.getField(mongodbBatchConfigurer.getJobLauncher(), "taskExecutor") instanceof SyncTaskExecutor);
    }
}