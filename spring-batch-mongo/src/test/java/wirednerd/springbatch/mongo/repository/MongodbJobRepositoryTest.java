package wirednerd.springbatch.mongo.repository;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.assertj.core.util.Lists;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;
import wirednerd.springbatch.mongo.converter.ExecutionContextConverterTest;
import wirednerd.springbatch.mongo.converter.JobExecutionConverter;
import wirednerd.springbatch.mongo.converter.JobExecutionConverterTest;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

@SpringBootTest
@MongoUnitTest
class MongodbJobRepositoryTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    private MongodbJobRepository repository;

    private JobExecution jobExecution;

    private final String jobCollectionName = "testJobs";
    private final String counterCollectionName = "testCounters";

    private static final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    private Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(MongodbJobRepository.class);
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void loggerSetup() {
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void loggerDetach() {
        logger.detachAppender(appender);
        appender.stop();
    }

    @BeforeEach
    void setUp() {
        repository = new MongodbJobRepository(mongoTemplate, jobCollectionName, counterCollectionName);
        jobExecution = JobExecutionConverterTest.buildJobExecution();
        jobExecution.setId(0L);
        jobExecution.getJobInstance().setId(0L);
        jobExecution.getStepExecutions().forEach(step -> step.setId(0L));

        jobExecution.setStatus(BatchStatus.COMPLETED);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);
    }

    @Test
    void constructor() {
        repository = new MongodbJobRepository(mongoTemplate, "A", "B");
        assertEquals("A", repository.getJobCollectionName());
        assertEquals("B", repository.getCounterCollectionName());
    }

    @Test
    void constructor_counters() {
        repository = new MongodbJobRepository(mongoTemplate, jobCollectionName, counterCollectionName);

        assertEquals(JOB_INSTANCE_ID, repository.getJobInstanceCounter().getCounterName());
        assertEquals(counterCollectionName, repository.getJobInstanceCounter().getCounterCollection());
        assertEquals(1L, repository.getJobInstanceCounter().nextValue());

        assertEquals(JOB_EXECUTION_ID, repository.getJobExecutionCounter().getCounterName());
        assertEquals(counterCollectionName, repository.getJobExecutionCounter().getCounterCollection());
        assertEquals(1L, repository.getJobExecutionCounter().nextValue());

        assertEquals(STEP_EXECUTION_ID, repository.getStepExecutionCounter().getCounterName());
        assertEquals(counterCollectionName, repository.getStepExecutionCounter().getCounterCollection());
        assertEquals(1L, repository.getStepExecutionCounter().nextValue());
    }

    @Test
    void isJobInstanceExists() {
        assertTrue(repository.isJobInstanceExists(jobExecution.getJobInstance().getJobName(), jobExecution.getJobParameters()));

        assertFalse(repository.isJobInstanceExists("Not Found", jobExecution.getJobParameters()));

        assertFalse(repository.isJobInstanceExists(jobExecution.getJobInstance().getJobName(), new JobParameters()));
    }

    @Test
    void isJobInstanceExists_nullJobName() {
        try {
            repository.isJobInstanceExists(null, new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Job name must not be null or empty.", e.getMessage());
        }
    }

    @Test
    void isJobInstanceExists_nullJobParameters() {
        try {
            repository.isJobInstanceExists("Not Found", null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobParameters must not be null.", e.getMessage());
        }
    }

    @Test
    void createJobInstance() {
        var jobInstance = repository.createJobInstance("New Job", jobExecution.getJobParameters());

        assertEquals(1L, jobInstance.getId());
        assertEquals(1L, jobInstance.getInstanceId());
        assertEquals("New Job", jobInstance.getJobName());

        var documents = mongoTemplate.find(new Query(Criteria.where("jobInstanceId").is(1L)), Document.class, jobCollectionName);
        assertEquals(1, documents.size());
        assertEquals(jobKeyGenerator.generateKey(jobExecution.getJobParameters()), documents.get(0).getString("jobKey"));

        var jobExecution = JobExecutionConverter.convert(documents.get(0));
        assertEquals(1L, jobExecution.getJobInstance().getId());
        assertEquals("New Job", jobExecution.getJobInstance().getJobName());
    }

    @Test
    void createJobInstance_noJobName() {
        try {
            repository.createJobInstance("", new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Job name must not be null or empty.", e.getMessage());
        }
    }

    @Test
    void createJobInstance_nullParameters() {
        try {
            repository.createJobInstance("New Job", null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobParameters must not be null.", e.getMessage());
        }
    }

    @Test
    void createJobInstance_existingJob() {
        try {
            repository.createJobInstance(jobExecution.getJobInstance().getJobName(), jobExecution.getJobParameters());
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("JobInstance must not already exist.", e.getMessage());
        }
    }

    @Test
    void createJobExecutionWithConfigName() {
        var newExecution = repository.createJobExecution(new JobInstance(10L, "New Job"), jobExecution.getJobParameters(), jobExecution.getJobConfigurationName());

        assertEquals("New Job", newExecution.getJobInstance().getJobName());
        assertEquals(10L, newExecution.getJobId());
        assertEquals(0, newExecution.getVersion());
        assertEquals(jobExecution.getJobParameters(), newExecution.getJobParameters());
        assertEquals(jobExecution.getJobConfigurationName(), newExecution.getJobConfigurationName());
        assertEquals(0, newExecution.getExecutionContext().entrySet().size());
        assertNotNull(newExecution.getLastUpdated());
        assertEquals(1L, newExecution.getId());

        assertTrue(repository.isJobInstanceExists("New Job", jobExecution.getJobParameters()));
    }

    @Test
    void createJobExecutionWithConfigName_replaceJobInstance() {
        var jobInstance = repository.createJobInstance("New Job", jobExecution.getJobParameters());

        assertEquals(1, mongoTemplate.find(new Query(Criteria.where("jobInstanceId").is(jobInstance.getId())).addCriteria(Criteria.where(JOB_EXECUTION_ID).isNull()), Document.class, jobCollectionName).size());
        assertEquals(0, mongoTemplate.find(new Query(Criteria.where("jobInstanceId").is(jobInstance.getId())).addCriteria(Criteria.where(JOB_EXECUTION_ID).gte(0L)), Document.class, jobCollectionName).size());

        var jobExecution = repository.createJobExecution(jobInstance, this.jobExecution.getJobParameters(), this.jobExecution.getJobConfigurationName());

        assertEquals(0, mongoTemplate.find(new Query(Criteria.where("jobInstanceId").is(jobInstance.getId())).addCriteria(Criteria.where(JOB_EXECUTION_ID).isNull()), Document.class, jobCollectionName).size());
        assertEquals(1, mongoTemplate.find(new Query(Criteria.where("jobInstanceId").is(jobInstance.getId())).addCriteria(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName).size());
    }

    @Test
    void createJobExecutionWithConfigName_nullJobInstance() {
        try {
            repository.createJobExecution(null, jobExecution.getJobParameters(), jobExecution.getJobConfigurationName());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A JobInstance is required to associate the JobExecution with", e.getMessage());
        }
    }

    @Test
    void createJobExecutionWithConfigName_nullJobParameters() {
        try {
            repository.createJobExecution(new JobInstance(10L, "New Job"), null, jobExecution.getJobConfigurationName());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A JobParameters object is required to create a JobExecution", e.getMessage());
        }
    }

    @Test
    void createJobExecutionWithConfigName_nullJobInstanceId() {
        try {
            jobExecution.getJobInstance().setId(null);
            repository.createJobExecution(jobExecution.getJobInstance(), jobExecution.getJobParameters(), jobExecution.getJobConfigurationName());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A jobInstanceId is required to create a JobExecution", e.getMessage());
        }
    }

    @Test
    void createJobExecution_newJobInstance() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        var jobExecution = repository.createJobExecution("New Job", new JobParameters(paramMap));

        assertEquals("New Job", jobExecution.getJobInstance().getJobName());
        assertEquals("Test Value", jobExecution.getJobParameters().getString("Test String Key"));
        assertEquals(1L, jobExecution.getJobId());
        assertNull(jobExecution.getJobConfigurationName());
        assertNotNull(jobExecution.getLastUpdated());
        assertEquals(1l, jobExecution.getId());
        assertEquals(0, jobExecution.getVersion());
        assertEquals(0, jobExecution.getExecutionContext().size());

        var jobExecutionDoc = mongoTemplate.findOne(new Query(Criteria.where(JOB_EXECUTION_ID).is(1L)), Document.class, jobCollectionName);
        assertEquals("New Job", jobExecutionDoc.getString(JOB_NAME));
        assertEquals(jobKeyGenerator.generateKey(jobExecution.getJobParameters()), jobExecutionDoc.getString(JOB_KEY));
    }

    @Test
    void createJobExecution_jobInstanceExistsButNoExecutions() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        repository.createJobInstance("New Job", new JobParameters(paramMap));

        try {
            repository.createJobExecution("New Job", new JobParameters(paramMap));
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Cannot find any job execution for jobName=New Job jobKey=" + jobKeyGenerator.generateKey(new JobParameters(paramMap)), e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForCompletedJob_UpdatedParameters() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>(jobExecution.getJobParameters().getParameters());
        paramMap.put("Test String Key", new JobParameter("Changed Value"));
        var newJobExecution = repository.createJobExecution(jobExecution.getJobInstance().getJobName(), new JobParameters(paramMap));

        assertEquals(jobExecution.getJobInstance().getJobName(), newJobExecution.getJobInstance().getJobName());
        assertEquals("Changed Value", newJobExecution.getJobParameters().getString("Test String Key"));
        assertEquals(1L, newJobExecution.getJobId());
        assertNull(newJobExecution.getJobConfigurationName());
        assertNotNull(newJobExecution.getLastUpdated());
        assertEquals(1l, newJobExecution.getId());
        assertEquals(0, newJobExecution.getVersion());
        assertEquals(0, newJobExecution.getExecutionContext().size());

        var jobExecutionDoc = mongoTemplate.findOne(new Query(Criteria.where(JOB_EXECUTION_ID).is(1L)), Document.class, jobCollectionName);
        assertEquals(jobExecution.getJobInstance().getJobName(), jobExecutionDoc.getString(JOB_NAME));
        assertEquals(jobKeyGenerator.generateKey(newJobExecution.getJobParameters()), jobExecutionDoc.getString(JOB_KEY));
    }

    @Test
    void createJobExecution_newExecutionForCompletedJob_NoIdentifyingParameters() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L, false));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.COMPLETED);
        jobExecution.getExecutionContext().putString("String", "String Value");
        jobExecution.getExecutionContext().putLong("Long", 789L);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        jobExecution.setId(repository.getJobExecutionCounter().nextValue());
        jobExecution.getExecutionContext().putString("String2", "value2");

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        var newJobExecution = repository.createJobExecution(jobInstance.getJobName(), jobParameters);

        assertEquals(jobExecution.getJobInstance().getJobName(), newJobExecution.getJobInstance().getJobName());
        assertEquals(123L, newJobExecution.getJobParameters().getLong("Test Long Key"));
        assertEquals(jobInstance.getId(), newJobExecution.getJobId());
        assertNull(newJobExecution.getJobConfigurationName());
        assertNotNull(newJobExecution.getLastUpdated());
        assertNotEquals(jobExecution.getId(), newJobExecution.getId());
        assertEquals(0, newJobExecution.getVersion());
        assertEquals(jobExecution.getExecutionContext(), newJobExecution.getExecutionContext());
        assertEquals(3, newJobExecution.getExecutionContext().size());

        var jobExecutionDoc = mongoTemplate.findOne(new Query(Criteria.where(JOB_EXECUTION_ID).is(newJobExecution.getId())), Document.class, jobCollectionName);
        assertEquals(jobExecution.getJobInstance().getJobName(), jobExecutionDoc.getString(JOB_NAME));
        assertEquals(jobKeyGenerator.generateKey(newJobExecution.getJobParameters()), jobExecutionDoc.getString(JOB_KEY));
    }

    @Test
    void createJobExecution_newExecutionForAbandonedJob_NoIdentifyingParameters() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L, false));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.ABANDONED);
        jobExecution.getExecutionContext().putString("String", "String Value");
        jobExecution.getExecutionContext().putLong("Long", 789L);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        jobExecution.setId(repository.getJobExecutionCounter().nextValue());
        jobExecution.getExecutionContext().putString("String2", "value2");

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        var newJobExecution = repository.createJobExecution(jobInstance.getJobName(), jobParameters);

        assertEquals(jobExecution.getJobInstance().getJobName(), newJobExecution.getJobInstance().getJobName());
        assertEquals(123L, newJobExecution.getJobParameters().getLong("Test Long Key"));
        assertEquals(jobInstance.getId(), newJobExecution.getJobId());
        assertNull(newJobExecution.getJobConfigurationName());
        assertNotNull(newJobExecution.getLastUpdated());
        assertNotEquals(jobExecution.getId(), newJobExecution.getId());
        assertEquals(0, newJobExecution.getVersion());
        assertEquals(jobExecution.getExecutionContext(), newJobExecution.getExecutionContext());
        assertEquals(3, newJobExecution.getExecutionContext().size());

        var jobExecutionDoc = mongoTemplate.findOne(new Query(Criteria.where(JOB_EXECUTION_ID).is(newJobExecution.getId())), Document.class, jobCollectionName);
        assertEquals(jobExecution.getJobInstance().getJobName(), jobExecutionDoc.getString(JOB_NAME));
        assertEquals(jobKeyGenerator.generateKey(newJobExecution.getJobParameters()), jobExecutionDoc.getString(JOB_KEY));
    }

    @Test
    void createJobExecution_nullJobName() throws Exception {
        try {
            repository.createJobExecution("", new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Job name must not be null or empty.", e.getMessage());
        }
    }

    @Test
    void createJobExecution_nullJobParametersName() throws Exception {
        try {
            repository.createJobExecution("New Job", null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobParameters must not be null.", e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForRunningJob() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L, false));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.STARTED);
        jobExecution.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution.setEndTime(null);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        try {
            repository.createJobExecution(jobInstance.getJobName(), jobParameters);
            fail("JobExecutionAlreadyRunningException expected");
        } catch (JobExecutionAlreadyRunningException e) {
            assertEquals("A job execution for this job is already running. jobExecutionId=" + jobExecution.getId(), e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForStoppingJob() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L, false));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.STOPPING);
        jobExecution.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution.setEndTime(new Date(System.currentTimeMillis()));

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        try {
            repository.createJobExecution(jobInstance.getJobName(), jobParameters);
            fail("JobExecutionAlreadyRunningException expected");
        } catch (JobExecutionAlreadyRunningException e) {
            assertEquals("A job execution for this job is already running. jobExecutionId=" + jobExecution.getId(), e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForJobWithUnknownStatus() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L, false));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.UNKNOWN);
        jobExecution.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution.setEndTime(new Date(System.currentTimeMillis()));

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        try {
            repository.createJobExecution(jobInstance.getJobName(), jobParameters);
            fail("JobRestartException expected");
        } catch (JobRestartException e) {
            assertEquals("Cannot restart job from UNKNOWN status. "
                    + "The last execution ended with a failure that could not be rolled back, "
                    + "so it may be dangerous to proceed. Manual intervention is probably necessary."
                    + " jobExecutionId=" + jobExecution.getId(), e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForJobWithCompletedStatus_WithIdentifyingParameters() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.COMPLETED);
        jobExecution.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution.setEndTime(new Date(System.currentTimeMillis()));

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        try {
            repository.createJobExecution(jobInstance.getJobName(), jobParameters);
            fail("JobInstanceAlreadyCompleteException expected");
        } catch (JobInstanceAlreadyCompleteException e) {
            assertEquals("A job instance already exists and is complete."
                    + " If you want to run this job again, change the identifying parameters."
                    + " jobExecutionId=" + jobExecution.getId(), e.getMessage());
        }
    }

    @Test
    void createJobExecution_newExecutionForJobWithAbandonedStatus_WithIdentifyingParameters() throws Exception {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test Long Key", new JobParameter(123L));
        var jobParameters = new JobParameters(paramMap);
        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), "New Job");
        var jobExecution = new JobExecution(jobInstance, repository.getJobExecutionCounter().nextValue(), jobParameters, "Job Configuration String");
        jobExecution.setStatus(BatchStatus.ABANDONED);
        jobExecution.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution.setEndTime(new Date(System.currentTimeMillis()));

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution), jobCollectionName);

        try {
            repository.createJobExecution(jobInstance.getJobName(), jobParameters);
            fail("JobInstanceAlreadyCompleteException expected");
        } catch (JobInstanceAlreadyCompleteException e) {
            assertEquals("A job instance already exists and is complete."
                    + " If you want to run this job again, change the identifying parameters."
                    + " jobExecutionId=" + jobExecution.getId(), e.getMessage());
        }
    }

    @Test
    void update_sameVersion() {
        mongoTemplate.updateFirst(Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Update.update(STATUS, "COMPLETED"), jobCollectionName);
        jobExecution.setStatus(BatchStatus.STARTED);
        // since database has same version number, Status should be updated to in database "STARTED"

        var beforeLastUpdated = jobExecution.getLastUpdated();

        repository.update(jobExecution);

        assertEquals(BatchStatus.STARTED, jobExecution.getStatus());
        assertTrue(beforeLastUpdated.before(jobExecution.getLastUpdated()),
                beforeLastUpdated.toString() + " " + jobExecution.getLastUpdated().toString());

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(jobExecution.getLastUpdated(), updatedDoc.getDate(LAST_UPDATED));
        assertEquals("STARTED", updatedDoc.getString(STATUS));
    }

    @Test
    void update_differentVersion() {
        var beforeVersion = jobExecution.getVersion();

        mongoTemplate.updateFirst(Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Update.update(STATUS, "COMPLETED")
                        .set(VERSION, beforeVersion + 1), jobCollectionName);
        jobExecution.setStatus(BatchStatus.STARTED);
        // Since database has different (higher) version number,
        // Status should remain COMPLETED in DB and updated in memory

        var beforeLastUpdated = jobExecution.getLastUpdated();

        repository.update(jobExecution);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(beforeVersion + 2, jobExecution.getVersion());
        assertTrue(beforeLastUpdated.before(jobExecution.getLastUpdated()),
                beforeLastUpdated.toString() + " " + jobExecution.getLastUpdated().toString());

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(jobExecution.getLastUpdated(), updatedDoc.getDate(LAST_UPDATED));
        assertEquals("COMPLETED", updatedDoc.getString(STATUS));
        assertEquals(beforeVersion + 2, updatedDoc.getInteger(VERSION));
    }

    @Test
    void update_notFound() {
        jobExecution.setId(repository.getJobExecutionCounter().nextValue());

        try {
            repository.update(jobExecution);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Job Execution not found for jobExecutionId=1", e.getMessage());
        }
    }

    @Test
    void update_nullJobExecution() {
        try {
            repository.update((JobExecution) null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobExecution cannot be null.", e.getMessage());
        }
    }

    @Test
    void update_nullJobExecutionId() {
        jobExecution.setId(null);
        try {
            repository.update(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobExecution must be already saved (have an id assigned).", e.getMessage());
        }
    }

    @Test
    void update_nullVersion() {
        jobExecution.setVersion(null);
        try {
            repository.update(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobExecution version cannot be null. JobExecution must be saved before it can be updated", e.getMessage());
        }
    }

    @Test
    void updateJobExecution() {
        Integer currentVersion = jobExecution.getVersion();

        jobExecution.setCreateTime(Date.from(OffsetDateTime.of(2022, 02, 1, 1, 3, 4, 0, ZoneOffset.UTC).toInstant()));
        jobExecution.setStartTime(Date.from(OffsetDateTime.of(2022, 02, 1, 2, 3, 4, 0, ZoneOffset.UTC).toInstant()));
        jobExecution.setEndTime(Date.from(OffsetDateTime.of(2022, 02, 1, 3, 3, 4, 0, ZoneOffset.UTC).toInstant()));
        jobExecution.setLastUpdated(Date.from(OffsetDateTime.of(2022, 02, 1, 4, 3, 4, 0, ZoneOffset.UTC).toInstant()));
        jobExecution.setStatus(BatchStatus.FAILED);
        jobExecution.setExitStatus(new ExitStatus("FAILED", "updateJobExecution"));


        ReflectionTestUtils.invokeMethod(repository, "updateJobExecution", jobExecution);

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(currentVersion + 1, updatedDoc.getInteger(VERSION));
        assertEquals(jobExecution.getCreateTime(), updatedDoc.getDate(CREATE_TIME));
        assertEquals(jobExecution.getStartTime(), updatedDoc.getDate(START_TIME));
        assertEquals(jobExecution.getEndTime(), updatedDoc.getDate(END_TIME));
        assertEquals(jobExecution.getLastUpdated(), updatedDoc.getDate(LAST_UPDATED));
        assertEquals("FAILED", updatedDoc.getString(STATUS));
        assertEquals("FAILED", updatedDoc.getString(EXIT_CODE));
        assertEquals("updateJobExecution", updatedDoc.getString(EXIT_DESCRIPTION));
    }

    @Test
    void updateJobExecution_versionNotFound() {
        jobExecution.incrementVersion();  // wrong current version
        var currentVersion = jobExecution.getVersion();

        try {
            ReflectionTestUtils.invokeMethod(repository, "updateJobExecution", jobExecution);
            fail("OptimisticLockingFailureException expected");
        } catch (OptimisticLockingFailureException e) {
            assertEquals("Attempt to update job execution id="
                    + jobExecution.getId() + " with version=" + currentVersion
                    + " which was not found", e.getMessage());
        }
    }

    @Test
    void updateJobExecutionContext() {
        jobExecution.setExecutionContext(new ExecutionContext());
        jobExecution.getExecutionContext().put("Key", "Updated Value");

        repository.updateExecutionContext(jobExecution);

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(1, updatedDoc.get(EXECUTION_CONTEXT, Document.class).size());
        assertEquals("Updated Value", updatedDoc.get(EXECUTION_CONTEXT, Document.class).getString("Key"));
    }

    @Test
    void updateJobExecutionContext_noChange() {
        var expected = jobExecution.getExecutionContext();

        repository.updateExecutionContext(jobExecution);

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        var updatedJobExecution = JobExecutionConverter.convert(updatedDoc);

        ExecutionContextConverterTest.compare(expected, updatedJobExecution.getExecutionContext());
    }

    @Test
    void updateJobExecutionContext_notFound() {
        jobExecution.setId(99L);

        try {
            repository.updateExecutionContext(jobExecution);
            fail("IllegalStateException  expected");
        } catch (IllegalStateException e) {
            assertEquals("Unable to update Execution Context for missing Job Execution.  jobExecutionId=99", e.getMessage());
        }
    }

    @Test
    void updateJobExecutionContext_nullJobExecution() {
        try {
            repository.updateExecutionContext((JobExecution) null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobExecution cannot be null.", e.getMessage());
        }
    }

    @Test
    void updateJobExecutionContext_nullJobExecutionId() {
        jobExecution.setId(null);
        try {
            repository.updateExecutionContext(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobExecution must be already saved (have an id assigned).", e.getMessage());
        }
    }

    @Test
    void getLastJobExecution() throws Exception {
        var jobName = "New Job";
        var jobParameters = new JobParameters();

        var jobInstance = new JobInstance(repository.getJobInstanceCounter().nextValue(), jobName);
        var newJobExecution1 = repository.createJobExecution(jobName, jobParameters);
        var newJobExecution2 = repository.createJobExecution(jobName, jobParameters);
        var foundExecution = repository.getLastJobExecution(jobName, jobParameters);

        assertNotEquals(newJobExecution1.getId(), newJobExecution2.getId());

        assertEquals(newJobExecution2.getId(), foundExecution.getId());
    }

    @Test
    void getLastJobExecution_notFound() throws Exception {
        var jobName = "New Job";
        var jobParameters = new JobParameters();

        var foundExecution = repository.getLastJobExecution(jobName, jobParameters);

        assertNull(foundExecution);
    }

    @Test
    void getLastJobExecution_jobInstance() throws Exception {
        var jobName = "New Job";
        var jobParameters = new JobParameters();

        repository.createJobInstance(jobName, jobParameters);

        var foundExecution = repository.getLastJobExecution(jobName, jobParameters);

        assertNull(foundExecution);
    }

    @Test
    void getLastJobExecution_nullJobName() {
        try {
            repository.getLastJobExecution(null, new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Job name must not be null or empty.", e.getMessage());
        }
    }

    @Test
    void getLastJobExecution_nullJobParameters() {
        try {
            repository.getLastJobExecution("Not Found", null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobParameters must not be null.", e.getMessage());
        }
    }

    @Test
    void add() {
        var step2 = jobExecution.createStepExecution("Step 2");

        repository.add(step2);

        assertNotNull(step2.getLastUpdated());
        assertEquals(1L, step2.getId());

        var jobExecutionDoc = mongoTemplate.findOne(Query.query(Criteria
                        .where(JOB_EXECUTION_ID).is(step2.getJobExecutionId())),
                Document.class, jobCollectionName);

        jobExecution = JobExecutionConverter.convert(jobExecutionDoc);

        assertEquals(2, jobExecution.getStepExecutions().size());
        var actualStep2 = (StepExecution) jobExecution.getStepExecutions().toArray()[1];
        assertEquals("Step 2", actualStep2.getStepName());
        assertEquals(1L, actualStep2.getId());
        assertEquals(step2.getLastUpdated(), actualStep2.getLastUpdated());
    }

    @Test
    void add_null() {
        try {
            repository.add((StepExecution) null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution cannot be null.", e.getMessage());
        }
    }

    @Test
    void add_nullJobExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        jobExecution.setId(null);

        try {
            repository.add(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution must belong to persisted JobExecution.", e.getMessage());
        }
    }

    @Test
    void add_WithStepExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        step2.setId(1L);

        try {
            repository.add(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("to-be-saved (not updated) StepExecution can't already have an id assigned", e.getMessage());
        }
    }

    @Test
    void addAll() {
        var step2 = jobExecution.createStepExecution("Step 2");
        var step3 = jobExecution.createStepExecution("Step 3");

        repository.addAll(Lists.newArrayList(step2, step3));

        assertNotNull(step2.getLastUpdated());
        assertEquals(1L, step2.getId());

        assertNotNull(step3.getLastUpdated());
        assertEquals(2L, step3.getId());

        var jobExecutionDoc = mongoTemplate.findOne(Query.query(Criteria
                        .where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Document.class, jobCollectionName);

        jobExecution = JobExecutionConverter.convert(jobExecutionDoc);

        assertEquals(3, jobExecution.getStepExecutions().size());

        var actualStep2 = (StepExecution) jobExecution.getStepExecutions().toArray()[1];
        assertEquals("Step 2", actualStep2.getStepName());
        assertEquals(1L, actualStep2.getId());
        assertEquals(step2.getLastUpdated(), actualStep2.getLastUpdated());

        var actualStep3 = (StepExecution) jobExecution.getStepExecutions().toArray()[2];
        assertEquals("Step 3", actualStep3.getStepName());
        assertEquals(2L, actualStep3.getId());
        assertEquals(step3.getLastUpdated(), actualStep3.getLastUpdated());
    }

    @Test
    void addAll_null() {
        repository.addAll(null);  // no errors thrown
        repository.addAll(Lists.newArrayList());  // no errors thrown
    }

    @Test
    void updateStep() {
        var step2 = jobExecution.createStepExecution("Step 2");
        var step3 = jobExecution.createStepExecution("Step 3");

        step2.getExecutionContext().put("Existing", "Existing");

        repository.add(step2);
        repository.add(step3);

        assertEquals(1L, step2.getId());
        assertEquals(2L, step3.getId());

        step2.setCommitCount(9999);
        step2.getExecutionContext().put("TestKey", "TestValue");
        var lastUpdatedBefore = step2.getLastUpdated();

        repository.update(step2);


        var jobExecutionDoc = mongoTemplate.findOne(Query.query(Criteria
                        .where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Document.class, jobCollectionName);

        var updatedJobExecution = JobExecutionConverter.convert(jobExecutionDoc);

        var actualStep1 = (StepExecution) updatedJobExecution.getStepExecutions().toArray()[0];
        assertEquals("Example Step", actualStep1.getStepName());
        assertNotEquals(9999, actualStep1.getCommitCount());

        var actualStep3 = (StepExecution) updatedJobExecution.getStepExecutions().toArray()[2];
        assertEquals("Step 3", actualStep3.getStepName());
        assertNotEquals(9999, actualStep3.getCommitCount());

        var actualStep2 = (StepExecution) updatedJobExecution.getStepExecutions().toArray()[1];
        assertEquals("Step 2", actualStep2.getStepName());
        assertEquals(9999, actualStep2.getCommitCount());
        assertNotEquals(lastUpdatedBefore, actualStep2.getLastUpdated());
        assertEquals(1, actualStep2.getExecutionContext().size());
        assertEquals("Existing", actualStep2.getExecutionContext().getString("Existing"));

        assertEquals(jobExecution.getVersion(), updatedJobExecution.getVersion());
    }

    @Test
    void updateStep_null() {
        try {
            repository.update((StepExecution) null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution cannot be null.", e.getMessage());
        }
    }

    @Test
    void updateStep_nullJobExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        jobExecution.setId(null);

        try {
            repository.update(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution must belong to persisted JobExecution.", e.getMessage());
        }
    }

    @Test
    void updateStep_nullStepExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        step2.setId(null);

        try {
            repository.update(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution must already be saved (have an id assigned)", e.getMessage());
        }
    }

    @Test
    void updateStep_sameVersion() {
        mongoTemplate.updateFirst(Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Update.update(STATUS, "COMPLETED"), jobCollectionName);
        jobExecution.setStatus(BatchStatus.STARTED);
        // since database has same version number, Status in memory will remain STARTED.
        // jobExecution data is not updated in database in this operation, so remains COMPLETED.

        jobExecution.getStepExecutions().iterator().next().setCommitCount(9999);
        repository.update(jobExecution.getStepExecutions().iterator().next());

        assertEquals(BatchStatus.STARTED, jobExecution.getStatus());

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(jobExecution.getLastUpdated(), updatedDoc.getDate(LAST_UPDATED));
        assertEquals("COMPLETED", updatedDoc.getString(STATUS));

        assertEquals(9999, updatedDoc.getList(STEP_EXECUTIONS, Document.class).get(0).getInteger(COMMIT_COUNT));
    }

    @Test
    void updateStep_differentVersion() {
        var beforeVersion = jobExecution.getVersion();

        mongoTemplate.updateFirst(Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Update.update(STATUS, "COMPLETED")
                        .set(VERSION, beforeVersion + 1), jobCollectionName);
        jobExecution.setStatus(BatchStatus.STARTED);
        // Since database has different (higher) version number,
        // Status should remain COMPLETED in DB and updated in memory

        jobExecution.getStepExecutions().iterator().next().setCommitCount(9999);
        repository.update(jobExecution.getStepExecutions().iterator().next());

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(beforeVersion + 2, jobExecution.getVersion());

        var updatedDoc = mongoTemplate.findOne(Query
                .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())), Document.class, jobCollectionName);

        assertEquals(jobExecution.getLastUpdated(), updatedDoc.getDate(LAST_UPDATED));
        assertEquals("COMPLETED", updatedDoc.getString(STATUS));
        assertEquals(beforeVersion + 2, updatedDoc.getInteger(VERSION));

        assertEquals(9999, updatedDoc.getList(STEP_EXECUTIONS, Document.class).get(0).getInteger(COMMIT_COUNT));
    }

    @Test
    void updateStep_notFound() {
        jobExecution.setId(repository.getJobExecutionCounter().nextValue());

        try {
            repository.update(jobExecution.getStepExecutions().iterator().next());
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Job Execution not found for jobExecutionId=1", e.getMessage());
        }
    }

    @Test
    void updateStepExecution_versionNotFound() {
        jobExecution.incrementVersion();  // wrong current version
        var currentVersion = jobExecution.getVersion();

        try {
            ReflectionTestUtils.invokeMethod(repository, "updateStepExecution", jobExecution.getStepExecutions().iterator().next());
            fail("OptimisticLockingFailureException expected");
        } catch (OptimisticLockingFailureException e) {
            assertEquals("Attempt to update job execution id="
                    + jobExecution.getId() + " with version=" + currentVersion
                    + " which was not found", e.getMessage());
        }
    }

    @Test
    void updateStep_JobIsStopping() {
        jobExecution.setStatus(BatchStatus.STOPPING);

        var step = jobExecution.getStepExecutions().iterator().next();
        assertFalse(step.isTerminateOnly());
        repository.update(step);

        assertTrue(step.isTerminateOnly());

        assertEquals(1, appender.list.size());
        assertEquals(Level.INFO, appender.list.get(0).getLevel());
        assertEquals("Parent JobExecution is stopped, so passing message on to StepExecution",
                appender.list.get(0).getMessage());
    }

    @Test
    void updateStepExecutionContext() {
        var step2 = jobExecution.createStepExecution("Step 2");
        var step3 = jobExecution.createStepExecution("Step 3");

        step2.getExecutionContext().put("Existing", "Existing");

        repository.add(step2);
        repository.add(step3);

        assertEquals(1L, step2.getId());
        assertEquals(2L, step3.getId());

        step2.getExecutionContext().put("TestKey", "TestValue");
        var lastUpdatedBefore = step2.getLastUpdated();

        repository.updateExecutionContext(step2);

        var jobExecutionDoc = mongoTemplate.findOne(Query.query(Criteria
                        .where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Document.class, jobCollectionName);

        var updatedJobExecution = JobExecutionConverter.convert(jobExecutionDoc);

        var actualStep3 = (StepExecution) updatedJobExecution.getStepExecutions().toArray()[2];
        assertEquals("Step 3", actualStep3.getStepName());
        assertEquals(0, actualStep3.getExecutionContext().size());

        var actualStep2 = (StepExecution) updatedJobExecution.getStepExecutions().toArray()[1];
        assertEquals("Step 2", actualStep2.getStepName());
        assertEquals(2, actualStep2.getExecutionContext().size());
        assertEquals("Existing", actualStep2.getExecutionContext().getString("Existing"));
        assertEquals("TestValue", actualStep2.getExecutionContext().getString("TestKey"));
    }

    @Test
    void updateStepExecutionContext_null() {
        try {
            repository.updateExecutionContext((StepExecution) null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution cannot be null.", e.getMessage());
        }
    }

    @Test
    void updateStepExecutionContext_nullJobExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        jobExecution.setId(null);

        try {
            repository.updateExecutionContext(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution must belong to persisted JobExecution.", e.getMessage());
        }
    }

    @Test
    void updateStepExecutionContext_nullStepExecutionId() {
        var step2 = jobExecution.createStepExecution("Step 2");
        step2.setId(null);

        try {
            repository.updateExecutionContext(step2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("StepExecution must already be saved (have an id assigned)", e.getMessage());
        }
    }

    private JobInstance getLastStepExecution_setup() {
        var jobInstance = new JobInstance(10L, "Job1");
        var jobExecution1 = new JobExecution(jobInstance, 11L, new JobParameters(), "");
        StepExecution step;
        step = jobExecution1.createStepExecution("Step1");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(11L);

        step = jobExecution1.createStepExecution("Step2");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(12L);

        step = jobExecution1.createStepExecution("Step3");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(13L);

        var jobExecution2 = new JobExecution(jobInstance, 12L, new JobParameters(), "");
        step = jobExecution2.createStepExecution("Step1");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(21L);

        step = jobExecution2.createStepExecution("Step2");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(22L);

        step = jobExecution2.createStepExecution("Step3");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(23L);

        step = jobExecution2.createStepExecution("Step3");
        step.setStartTime(Date.from(OffsetDateTime.of(2020, 2, 4, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        step.setId(24L);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution1), jobCollectionName);
        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution2), jobCollectionName);

        return jobInstance;
    }

    @Test
    void getLastStepExecution_highStartDate() {
        var jobInstance = getLastStepExecution_setup();
        // Choose Step with the highest start date
        var foundStep = repository.getLastStepExecution(jobInstance, "Step2");
        assertEquals(12L, foundStep.getJobExecutionId());
        assertEquals(22L, foundStep.getId());
    }

    @Test
    void getLastStepExecution_highExecutionId() {
        var jobInstance = getLastStepExecution_setup();
        // Steps have the same start date, choose value with the highest stepExecutionId
        var foundStep = repository.getLastStepExecution(jobInstance, "Step1");
        assertEquals(12L, foundStep.getJobExecutionId());
        assertEquals(21L, foundStep.getId());
    }

    @Test
    void getLastStepExecution_repeatedStep() {
        var jobInstance = getLastStepExecution_setup();
        // Step exists multiple times on same job
        var foundStep = repository.getLastStepExecution(jobInstance, "Step3");
        assertEquals(12L, foundStep.getJobExecutionId());
        assertEquals(24L, foundStep.getId());
    }

    @Test
    void getLastStepExecution_notFound() {
        assertNull(repository.getLastStepExecution(jobExecution.getJobInstance(), "StepZ"));
    }

    @Test
    void getLastStepExecution_nullJobInstanceId() {
        try {
            repository.getLastStepExecution(new JobInstance(null, "JobName"), "StepZ");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null.", e.getMessage());
        }
    }

    @Test
    void getLastStepExecution_nulStepName() {
        try {
            repository.getLastStepExecution(new JobInstance(10L, "JobName"), "");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("stepName must not be null or blank.", e.getMessage());
        }
    }

    @Test
    void getStepExecutionCount() {
        var jobInstance = getLastStepExecution_setup();

        assertEquals(2, repository.getStepExecutionCount(jobInstance, "Step1"));
        assertEquals(2, repository.getStepExecutionCount(jobInstance, "Step2"));
        assertEquals(3, repository.getStepExecutionCount(jobInstance, "Step3"));
        assertEquals(0, repository.getStepExecutionCount(jobInstance, "Step4"));
    }

    @Test
    void getStepExecutionCount_nullJobInstanceId() {
        try {
            repository.getStepExecutionCount(new JobInstance(null, "JobName"), "StepZ");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null.", e.getMessage());
        }
    }

    @Test
    void getStepExecutionCount_nulStepName() {
        try {
            repository.getStepExecutionCount(new JobInstance(10L, "JobName"), "");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("stepName must not be null or blank.", e.getMessage());
        }
    }
}