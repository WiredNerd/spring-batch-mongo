package wirednerd.springbatch.mongo.converter;

import org.assertj.core.util.Lists;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.CollectionUtils;
import wirednerd.springbatch.mongo.MongoDBContainerConfig;

import java.sql.Date;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

public class JobExecutionConverterTest extends MongoDBContainerConfig {

    private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    @Test
    void mongoInsertAndFind() {
        var expected = buildJobExecution();

        mongoTemplate.insert(JobExecutionConverter.convert(expected), "Test");
        var actual = JobExecutionConverter.convert(mongoTemplate.findOne(new Query(), Document.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_minimal() {
        var jobInstance = new JobInstance(1L, "Example Job");
        var expected = new JobExecution(jobInstance, 2L, null, null);
        expected.setCreateTime(null);
        expected.setExecutionContext(null);

        mongoTemplate.insert(JobExecutionConverter.convert(expected), "Test");
        var actual = JobExecutionConverter.convert(mongoTemplate.findOne(new Query(), Document.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void converters() {
        var expected = buildJobExecution();

        var document = JobExecutionConverter.convert(expected);

        assertTrue(document.containsKey("jobExecutionId"), document.toJson());
        assertTrue(document.containsKey("version"), document.toJson());
        assertTrue(document.containsKey("jobParameters"), document.toJson());
        assertTrue(document.containsKey("jobInstanceId"), document.toJson());
        assertTrue(document.containsKey("jobName"), document.toJson());

        assertTrue(document.containsKey("jobKey"), document.toJson());
        assertEquals(jobKeyGenerator.generateKey(expected.getJobParameters()), document.getString("jobKey"));

        assertTrue(document.containsKey("stepExecutions"), document.toJson());
        assertTrue(document.containsKey("status"), document.toJson());
        assertTrue(document.containsKey("startTime"), document.toJson());
        assertTrue(document.containsKey("createTime"), document.toJson());
        assertTrue(document.containsKey("endTime"), document.toJson());
        assertTrue(document.containsKey("lastUpdated"), document.toJson());
        assertTrue(document.containsKey("exitCode"), document.toJson());
        assertTrue(document.containsKey("exitDescription"), document.toJson());

        assertFalse(document.containsKey("failureExceptions"));

        var actual = JobExecutionConverter.convert(document);

        compare(expected, actual);
    }

    @Test
    void converters_JobExecutionToDocument_null_jobExecutionId() {
        var jobInstance = new JobInstance(1L, "Example Job");
        var expected = new JobExecution(jobInstance, null, null, null);

        try {
            JobExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobExecutionId must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobExecutionToDocument_null_jobInstance() {
        var expected = new JobExecution(null, 2L, null, null);

        try {
            JobExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstance must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobExecutionToDocument_null_jobInstanceId() {
        var jobInstance = new JobInstance(null, "Example Job");
        var expected = new JobExecution(jobInstance, 2L, null, null);

        try {
            JobExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobExecutionToDocument_null_status() {
        var jobInstance = new JobInstance(1L, "Example Job");
        var expected = new JobExecution(jobInstance, 2L, null, null);
        expected.setStatus(null);

        try {
            JobExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("status must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobExecutionToDocument_null_exitStatus() {
        var jobInstance = new JobInstance(1L, "Example Job");
        var expected = new JobExecution(jobInstance, 2L, null, null);
        expected.setExitStatus(null);

        try {
            JobExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("exitStatus must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobExecutionToDocument_minimal() {
        var jobInstance = new JobInstance(1L, "Example Job");
        var expected = new JobExecution(jobInstance, 2L, null, null);
        expected.setCreateTime(null);
        expected.setExecutionContext(null);

        var document = JobExecutionConverter.convert(expected);

        assertTrue(document.containsKey("jobExecutionId"));
        assertEquals(2L, document.get("jobExecutionId"));

        assertTrue(document.containsKey("version"));
        assertNull(document.get("version"));

        assertTrue(document.containsKey("jobParameters"));
        assertEquals(0, document.get("jobParameters", Document.class).size());

        assertTrue(document.containsKey("jobInstanceId"));
        assertEquals(1L, document.get("jobInstanceId"));

        assertTrue(document.containsKey("jobName"));
        assertEquals("Example Job", document.get("jobName"));

        assertTrue(document.containsKey("jobKey"));
        assertEquals(jobKeyGenerator.generateKey(new JobParameters()), document.get("jobKey"));

        assertTrue(document.containsKey("stepExecutions"));
        assertNotNull(document.get("stepExecutions"));

        assertTrue(document.containsKey("status"));
        assertNotNull(document.get("status"));

        assertTrue(document.containsKey("startTime"));
        assertNull(document.get("startTime"));

        assertTrue(document.containsKey("createTime"));
        assertNull(document.get("createTime"));

        assertTrue(document.containsKey("endTime"));
        assertNull(document.get("endTime"));

        assertTrue(document.containsKey("lastUpdated"));
        assertNull(document.get("lastUpdated"));

        assertTrue(document.containsKey("exitCode"));
        assertNotNull(document.get("exitCode"));

        assertTrue(document.containsKey("exitDescription"));
        assertNotNull(document.get("exitDescription"));

        assertFalse(document.containsKey("failureExceptions"));
    }

    @Test
    void converters_DocumentToJobExecution_emptyDocument() {
        try {
            JobExecutionConverter.convert(new Document());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("A jobName is required", e.getMessage());
        }
    }

    public static JobExecution buildJobExecution() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L, false));

        var jobParameters = new JobParameters(paramMap);

        var jobInstance = new JobInstance(1L, "Example Job");

        var jobExecution = new JobExecution(jobInstance, 2L, jobParameters, "Job Configuration String");

        var step1 = StepExecutionConverterTest.buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(step1));

        jobExecution.setVersion(5);
        jobExecution.setStatus(BatchStatus.STARTED);
        jobExecution.setStartTime(Date.from(OffsetDateTime.now().minusHours(3).toInstant()));
        jobExecution.setCreateTime(Date.from(OffsetDateTime.now().minusHours(2).toInstant()));
        jobExecution.setEndTime(Date.from(OffsetDateTime.now().minusHours(1).toInstant()));
        jobExecution.setLastUpdated(Date.from(OffsetDateTime.now().toInstant()));
        jobExecution.setExitStatus(ExitStatus.EXECUTING);
        jobExecution.getExitStatus().addExitDescription("Test Exit Description");

        jobExecution.getExecutionContext().putString("String", "String Value");
        jobExecution.getExecutionContext().putLong("Long", 789L);

        jobExecution.addFailureException(new RuntimeException("Test Exception"));

        return jobExecution;
    }

    private void compare(JobExecution expected, JobExecution actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getVersion(), actual.getVersion());
        JobParametersConverterTest.compare(expected.getJobParameters(), actual.getJobParameters());

        if (expected.getJobInstance() == null) {
            assertNull(actual.getJobInstance());
        } else {
            assertEquals(expected.getJobInstance().getInstanceId(), actual.getJobInstance().getInstanceId());
            assertEquals(expected.getJobInstance().getJobName(), actual.getJobInstance().getJobName());
        }

        if (CollectionUtils.isEmpty(expected.getStepExecutions())) {
            assertTrue(CollectionUtils.isEmpty(actual.getStepExecutions()));
        } else {
            assertEquals(expected.getStepExecutions().size(), actual.getStepExecutions().size());
            var expectedSteps = expected.getStepExecutions().toArray();
            var actualSteps = actual.getStepExecutions().toArray();
            for (int i = 0; i < expected.getStepExecutions().size(); i++) {
                StepExecutionConverterTest.compare((StepExecution) expectedSteps[i], (StepExecution) actualSteps[i]);
            }
        }

        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getCreateTime(), actual.getCreateTime());
        assertEquals(expected.getEndTime(), actual.getEndTime());
        assertEquals(expected.getLastUpdated(), actual.getLastUpdated());

        if (expected.getExitStatus() == null) {
            assertNull(actual.getExitStatus().getExitCode());
            assertEquals("", actual.getExitStatus().getExitDescription());
        } else {
            assertEquals(expected.getExitStatus().getExitCode(), actual.getExitStatus().getExitCode());
            assertEquals(expected.getExitStatus().getExitDescription(), actual.getExitStatus().getExitDescription());
        }

        ExecutionContextConverterTest.compare(expected.getExecutionContext(), actual.getExecutionContext());
        assertTrue(CollectionUtils.isEmpty(actual.getFailureExceptions()));
        assertEquals(expected.getJobConfigurationName(), actual.getJobConfigurationName());
    }
}