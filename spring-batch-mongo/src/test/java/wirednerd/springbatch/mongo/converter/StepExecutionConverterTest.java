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

import static org.junit.jupiter.api.Assertions.*;

class StepExecutionConverterTest extends MongoDBContainerConfig {

    @Test
    void mongoInsertAndFind() {
        var jobExecution = buildBaseJobExecution();
        var expected = buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));

        mongoTemplate.insert(StepExecutionConverter.convert(expected), "Test");
        var document = mongoTemplate.findOne(new Query(), Document.class, "Test");
        var actual = StepExecutionConverter.convert(document, jobExecution);

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_minimal() {
        var jobExecution = buildBaseJobExecution();
        var expected = new StepExecution("Example Step", jobExecution, 3L);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));
        expected.setExecutionContext(null);

        mongoTemplate.insert(StepExecutionConverter.convert(expected), "Test");
        var document = mongoTemplate.findOne(new Query(), Document.class, "Test");
        var actual = StepExecutionConverter.convert(document, jobExecution);

        compare(expected, actual);
    }

    @Test
    void converters() {
        var jobExecution = buildBaseJobExecution();
        var expected = buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));

        var document = StepExecutionConverter.convert(expected);

        assertTrue(document.containsKey("stepExecutionId"), document.toJson());
        assertTrue(document.containsKey("stepName"), document.toJson());
        assertTrue(document.containsKey("status"), document.toJson());
        assertTrue(document.containsKey("readCount"), document.toJson());
        assertTrue(document.containsKey("writeCount"), document.toJson());
        assertTrue(document.containsKey("commitCount"), document.toJson());
        assertTrue(document.containsKey("rollbackCount"), document.toJson());
        assertTrue(document.containsKey("readSkipCount"), document.toJson());
        assertTrue(document.containsKey("processSkipCount"), document.toJson());
        assertTrue(document.containsKey("writeSkipCount"), document.toJson());
        assertTrue(document.containsKey("startTime"), document.toJson());
        assertTrue(document.containsKey("endTime"), document.toJson());
        assertTrue(document.containsKey("lastUpdated"), document.toJson());
        assertTrue(document.containsKey("exitCode"), document.toJson());
        assertTrue(document.containsKey("exitDescription"), document.toJson());
        assertTrue(document.containsKey("executionContext"), document.toJson());
        assertTrue(document.containsKey("filterCount"), document.toJson());

        assertFalse(document.containsKey("failureExceptions"));

        var actual = StepExecutionConverter.convert(document, jobExecution);

        compare(expected, actual);
    }

    @Test
    void converters_StepExecutionToDocument_null_stepExecutionId() {
        var jobExecution = buildBaseJobExecution();
        var expected = buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));

        expected.setId(null);

        try {
            StepExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("stepExecutionId must not be null", e.getMessage());
        }
    }

    @Test
    void converters_StepExecutionToDocument_null_status() {
        var jobExecution = buildBaseJobExecution();
        var expected = buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));

        expected.setStatus(null);

        try {
            StepExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("status must not be null", e.getMessage());
        }
    }

    @Test
    void converters_StepExecutionToDocument_null_exitStatus() {
        var jobExecution = buildBaseJobExecution();
        var expected = buildStepExecution(jobExecution);
        jobExecution.addStepExecutions(Lists.newArrayList(expected));

        expected.setExitStatus(null);

        try {
            StepExecutionConverter.convert(expected);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("exitStatus must not be null", e.getMessage());
        }
    }

    @Test
    void convert_DocumentToStepExecution_nulls() {
        var jobExecution = buildBaseJobExecution();
        var stepExecution = new StepExecution("Example Step", jobExecution, 3L);
        stepExecution.setStatus(null);
        stepExecution.setExitStatus(null);
        stepExecution.setStartTime(null);

        var document = new Document();

        document.put("stepExecutionId", 3L);
        document.put("stepName", "Example Step");

        var actual = StepExecutionConverter.convert(document, jobExecution);

        stepExecution.setStatus(BatchStatus.UNKNOWN);

        compare(stepExecution, actual);
    }

    private static JobExecution buildBaseJobExecution() {
        var jobInstance = new JobInstance(1L, "Example Job");
        return new JobExecution(jobInstance, 2L, new JobParameters(), "Job Configuration String");
    }

    public static StepExecution buildStepExecution(JobExecution jobExecution) {
        var stepExecution = new StepExecution("Example Step", jobExecution, 3L);

        stepExecution.setReadCount(1);
        stepExecution.setWriteCount(2);
        stepExecution.setCommitCount(3);
        stepExecution.setRollbackCount(4);
        stepExecution.setReadSkipCount(5);
        stepExecution.setProcessSkipCount(6);
        stepExecution.setWriteSkipCount(7);
        stepExecution.setStartTime(Date.from(OffsetDateTime.now().minusHours(2).toInstant()));
        stepExecution.setEndTime(Date.from(OffsetDateTime.now().minusHours(1).toInstant()));
        stepExecution.setLastUpdated(Date.from(OffsetDateTime.now().toInstant()));

        stepExecution.getExecutionContext().putString("String", "String Value");
        stepExecution.getExecutionContext().putLong("Long", 789L);
        stepExecution.setExitStatus(ExitStatus.EXECUTING);
        stepExecution.getExitStatus().addExitDescription("Test Exit Description");
        stepExecution.setFilterCount(8);
        stepExecution.addFailureException(new RuntimeException("Test Exception"));

        return stepExecution;
    }

    public static void compare(StepExecution expected, StepExecution actual) {
        assertEquals(expected.getId(), actual.getId());

        if (expected.getJobExecution() == null) {
            assertNull(actual.getJobExecution());
        } else {
            assertEquals(expected.getJobExecution().getId(), actual.getJobExecution().getId());
        }

        assertEquals(expected.getStepName(), actual.getStepName());
        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getReadCount(), actual.getReadCount());
        assertEquals(expected.getWriteCount(), actual.getWriteCount());
        assertEquals(expected.getCommitCount(), actual.getCommitCount());
        assertEquals(expected.getRollbackCount(), actual.getRollbackCount());
        assertEquals(expected.getReadSkipCount(), actual.getReadSkipCount());
        assertEquals(expected.getProcessSkipCount(), actual.getProcessSkipCount());
        assertEquals(expected.getWriteSkipCount(), actual.getWriteSkipCount());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getEndTime(), actual.getEndTime());
        assertEquals(expected.getLastUpdated(), actual.getLastUpdated());

        ExecutionContextConverterTest.compare(expected.getExecutionContext(), actual.getExecutionContext());

        if (expected.getExitStatus() == null) {
            assertNull(actual.getExitStatus().getExitCode());
            assertEquals("", actual.getExitStatus().getExitDescription());
        } else {
            assertEquals(expected.getExitStatus().getExitCode(), actual.getExitStatus().getExitCode());
            assertEquals(expected.getExitStatus().getExitDescription(), actual.getExitStatus().getExitDescription());
        }

        assertFalse(actual.isTerminateOnly());
        assertEquals(expected.getFilterCount(), actual.getFilterCount());
        assertTrue(CollectionUtils.isEmpty(actual.getFailureExceptions()));
    }
}