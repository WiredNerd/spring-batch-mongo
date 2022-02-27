package io.github.wirednerd.springbatch.document;

import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobExecutionDocumentMapperTest extends MongoDBContainerConfig {

    private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();
    private final JobExecutionDocumentMapper jobExecutionDocumentMapper = new JobExecutionDocumentMapper();
    private final Date testDate = Date.from(OffsetDateTime.of(2022, 2, 19, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());

    private JobParameters jobParameters;
    private JobInstance jobInstance;
    private ExecutionContext executionContext;
    private StepExecution stepExecution;
    private JobExecution jobExecution;

    @BeforeEach
    void setupData() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L));

        jobParameters = new JobParameters(paramMap);

        executionContext = new ExecutionContext();
        executionContext.put("Context", "Value1");

        jobInstance = new JobInstance(1L, "Example Job");
        jobExecution = new JobExecution(jobInstance, 2L, jobParameters, "Job Configuration String");

        jobExecution.setVersion(3);
        jobExecution.setStatus(BatchStatus.STARTED);
        jobExecution.setStartTime(java.sql.Date.from(OffsetDateTime.now().minusHours(2).toInstant()));
        jobExecution.setCreateTime(java.sql.Date.from(OffsetDateTime.now().minusHours(3).toInstant()));
        jobExecution.setEndTime(java.sql.Date.from(OffsetDateTime.now().minusHours(1).toInstant()));
        jobExecution.setLastUpdated(java.sql.Date.from(OffsetDateTime.now().toInstant()));
        jobExecution.setExitStatus(new ExitStatus("Exit", "Desc"));
        jobExecution.setExecutionContext(executionContext);

        stepExecution = new StepExecution("Example Step", jobExecution, 3L);

        stepExecution.setReadCount(1);
        stepExecution.setWriteCount(2);
        stepExecution.setCommitCount(3);
        stepExecution.setRollbackCount(4);
        stepExecution.setReadSkipCount(5);
        stepExecution.setProcessSkipCount(6);
        stepExecution.setWriteSkipCount(7);
        stepExecution.setStartTime(java.sql.Date.from(OffsetDateTime.now().minusHours(2).toInstant()));
        stepExecution.setEndTime(java.sql.Date.from(OffsetDateTime.now().minusHours(1).toInstant()));
        stepExecution.setLastUpdated(java.sql.Date.from(OffsetDateTime.now().toInstant()));

        stepExecution.setExecutionContext(executionContext);
        stepExecution.setExitStatus(ExitStatus.EXECUTING);
        stepExecution.getExitStatus().addExitDescription("Test Exit Description");
        stepExecution.setFilterCount(8);
        stepExecution.addFailureException(new RuntimeException("Test Exception"));
    }

    @Test
    void toJobExecutionDocument() {
        var document = jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);

        assertEquals(jobExecution.getId(), document.getJobExecutionId());
        assertEquals(jobExecution.getVersion(), document.getVersion());
        assertEquals(jobParameters.getParameters().size(), document.getJobParameters().size());
        assertEquals(jobInstance.getId(), document.getJobInstanceId());
        assertEquals(jobInstance.getJobName(), document.getJobName());
        assertEquals(jobKeyGenerator.generateKey(jobParameters), document.getJobKey());
        assertEquals(1, document.getStepExecutions().size());
        assertEquals(jobExecution.getStatus().toString(), document.getStatus());
        assertEquals(jobExecution.getStartTime(), document.getStartTime());
        assertEquals(jobExecution.getCreateTime(), document.getCreateTime());
        assertEquals(jobExecution.getEndTime(), document.getEndTime());
        assertEquals(jobExecution.getLastUpdated(), document.getLastUpdated());
        assertEquals(jobExecution.getExitStatus().getExitCode(), document.getExitCode());
        assertEquals(jobExecution.getExitStatus().getExitDescription(), document.getExitDescription());
        assertEquals(jobExecutionDocumentMapper.serializeContext(executionContext), document.getExecutionContext());
        assertEquals(jobExecution.getJobConfigurationName(), document.getJobConfigurationName());
    }

    @Test
    void toJobExecutionDocumentNullJobExecutionId() {
        jobExecution.setId(null);

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobExecutionId must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentNullJobInstance() {
        jobExecution.setJobInstance(null);

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstance must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentNullJobInstanceId() {
        jobExecution.getJobInstance().setId(null);

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentNullStatus() {
        jobExecution.setStatus(null);

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("status must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentNullExitStatus() {
        jobExecution.setExitStatus(null);

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("exitStatus must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecution() {
        var document = jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
        var actual = jobExecutionDocumentMapper.toJobExecution(document);

        compare(jobExecution, actual);
    }

    @Test
    void toJobExecutionNoSteps() {
        var document = jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution);
        document.getStepExecutions().clear();
        var actual = jobExecutionDocumentMapper.toJobExecution(document);

        assertEquals(0, actual.getStepExecutions().size());
    }

    @Test
    void mongoInsertAndFindJobExecution() {
        mongoTemplate.insert(jobExecutionDocumentMapper.toJobExecutionDocument(jobExecution), "Test");
        var document = mongoTemplate.findOne(new Query(), JobExecutionDocument.class, "Test");
        var actual = jobExecutionDocumentMapper.toJobExecution(document);

        compare(jobExecution, actual);
    }

    @Test
    void toJobExecutionDocumentFromJobInstance() {
        var document = jobExecutionDocumentMapper.toJobExecutionDocument(jobInstance, jobParameters);

        assertNull(document.getJobExecutionId());
        assertNull(document.getVersion());
        assertNull(document.getJobParameters());
        assertEquals(jobInstance.getId(), document.getJobInstanceId());
        assertEquals(jobInstance.getJobName(), document.getJobName());
        assertEquals(jobKeyGenerator.generateKey(jobParameters), document.getJobKey());
        assertNull(document.getStepExecutions());
        assertNull(document.getStatus());
        assertNull(document.getStartTime());
        assertNull(document.getCreateTime());
        assertNull(document.getEndTime());
        assertNull(document.getLastUpdated());
        assertNull(document.getExitCode());
        assertNull(document.getExitDescription());
        assertNull(document.getExecutionContext());
        assertNull(document.getJobConfigurationName());
    }

    @Test
    void toJobExecutionDocumentFromJobInstanceNullJobInstance() {
        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(null, jobParameters);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstance must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentFromJobInstanceNullJobInstanceId() {
        jobInstance = new JobInstance(null, "Example Job");

        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobInstance, jobParameters);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null", e.getMessage());
        }
    }

    @Test
    void toJobExecutionDocumentFromJobInstanceNullJobParameters() {
        try {
            jobExecutionDocumentMapper.toJobExecutionDocument(jobInstance, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobParameters must not be null", e.getMessage());
        }
    }

    @Test
    void toJobInstanceFromJobExecutionDocument() {
        var document = jobExecutionDocumentMapper.toJobExecutionDocument(jobInstance, jobParameters);
        var actual = jobExecutionDocumentMapper.toJobExecution(document).getJobInstance();

        compare(jobInstance, actual);
    }

    @Test
    void toJobInstanceDocument() {
        var document = jobExecutionDocumentMapper.toJobInstanceDocument(jobInstance, jobParameters);

        assertEquals(jobInstance.getId(), document.getJobInstanceId());
        assertEquals(jobInstance.getJobName(), document.getJobName());
        assertEquals(jobKeyGenerator.generateKey(jobParameters), document.getJobKey());
    }

    @Test
    void toJobInstanceDocumentNullJobInstance() {
        try {
            jobExecutionDocumentMapper.toJobInstanceDocument(null, jobParameters);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstance must not be null", e.getMessage());
        }
    }

    @Test
    void toJobInstanceDocumentNullJobInstanceId() {
        jobInstance = new JobInstance(null, "Example Job");

        try {
            jobExecutionDocumentMapper.toJobInstanceDocument(jobInstance, jobParameters);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null", e.getMessage());
        }
    }

    @Test
    void toJobInstanceDocumentNullJobParameters() {
        try {
            jobExecutionDocumentMapper.toJobInstanceDocument(jobInstance, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobParameters must not be null", e.getMessage());
        }
    }

    @Test
    void toJobInstance() {
        var document = jobExecutionDocumentMapper.toJobInstanceDocument(jobInstance, jobParameters);
        var actual = jobExecutionDocumentMapper.toJobInstance(document);

        compare(jobInstance, actual);
    }

    @Test
    void mongoInsertAndFindJobInstance() {
        mongoTemplate.insert(jobExecutionDocumentMapper.toJobInstanceDocument(jobInstance, jobParameters), "Test");
        var document = mongoTemplate.findOne(new Query(), JobInstanceDocument.class, "Test");
        var actual = jobExecutionDocumentMapper.toJobInstance(document);

        compare(jobInstance, actual);
    }

    @Test
    void toStepExecutionDocument() {
        var document = jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);

        assertEquals(stepExecution.getId(), document.getStepExecutionId());
        assertEquals(stepExecution.getStepName(), document.getStepName());
        assertEquals(stepExecution.getStatus().toString(), document.getStatus());
        assertEquals(stepExecution.getReadCount(), document.getReadCount());
        assertEquals(stepExecution.getWriteCount(), document.getWriteCount());
        assertEquals(stepExecution.getCommitCount(), document.getCommitCount());
        assertEquals(stepExecution.getRollbackCount(), document.getRollbackCount());
        assertEquals(stepExecution.getReadSkipCount(), document.getReadSkipCount());
        assertEquals(stepExecution.getProcessSkipCount(), document.getProcessSkipCount());
        assertEquals(stepExecution.getWriteSkipCount(), document.getWriteSkipCount());
        assertEquals(stepExecution.getStartTime(), document.getStartTime());
        assertEquals(stepExecution.getEndTime(), document.getEndTime());
        assertEquals(stepExecution.getLastUpdated(), document.getLastUpdated());
        assertEquals(jobExecutionDocumentMapper.serializeContext(stepExecution.getExecutionContext()),
                document.getExecutionContext());
        assertEquals(stepExecution.getExitStatus().getExitCode(), document.getExitCode());
        assertEquals(stepExecution.getExitStatus().getExitDescription(), document.getExitDescription());
        assertEquals(stepExecution.getFilterCount(), document.getFilterCount());
    }

    @Test
    void toStepExecutionDocumentMinimal() {
        stepExecution = new StepExecution("Example Step", jobExecution, 3L);

        var document = jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);
        var actual = jobExecutionDocumentMapper.toStepExecution(document, jobExecution);

        compare(actual, stepExecution);
    }

    @Test
    void toStepExecutionDocumentWithNullStepExecutionId() {
        stepExecution.setId(null);

        try {
            jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("stepExecutionId must not be null", e.getMessage());
        }
    }

    @Test
    void toStepExecutionDocumentWithNullStatus() {
        stepExecution.setStatus(null);

        try {
            jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("status must not be null", e.getMessage());
        }
    }

    @Test
    void toStepExecutionDocumentWithNullExitStatus() {
        stepExecution.setExitStatus(null);

        try {
            jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("exitStatus must not be null", e.getMessage());
        }
    }

    @Test
    void toStepExecution() {
        var document = jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution);
        var actual = jobExecutionDocumentMapper.toStepExecution(document, jobExecution);

        compare(stepExecution, actual);
    }

    @Test
    void toStepExecutionWithMinimalData() {
        var document = new StepExecutionDocument();

        document.setStepExecutionId(3L);
        document.setStepName("Example Step");

        var actual = jobExecutionDocumentMapper.toStepExecution(document, jobExecution);

        var stepExecution = new StepExecution("Example Step", jobExecution, 3L);
        stepExecution.setStatus(null);
        stepExecution.setExitStatus(null);
        stepExecution.setStartTime(null);
        stepExecution.setStatus(BatchStatus.UNKNOWN);

        compare(stepExecution, actual);
    }

    @Test
    void mongoInsertAndFindStepExecution() {
        mongoTemplate.insert(jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution), "Test");
        var document = mongoTemplate.findOne(new Query(), StepExecutionDocument.class, "Test");
        var actual = jobExecutionDocumentMapper.toStepExecution(document, jobExecution);

        compare(stepExecution, actual);
    }

    @Test
    void mongoInsertAndFindStepExecutionMinimal() {
        stepExecution = new StepExecution("Example Step", jobExecution, 3L);

        mongoTemplate.insert(jobExecutionDocumentMapper.toStepExecutionDocument(stepExecution), "Test");
        var document = mongoTemplate.findOne(new Query(), StepExecutionDocument.class, "Test");
        var actual = jobExecutionDocumentMapper.toStepExecution(document, jobExecution);

        compare(stepExecution, actual);
    }

    @Test
    void toJobParametersDocument() {
        var document = jobExecutionDocumentMapper.toJobParametersDocument(jobParameters);

        assertEquals(2, document.keySet().size());
        assertTrue(document.containsKey("Test String Key"));
        assertEquals("Test Value", document.get("Test String Key").getStringValue());
        assertTrue(document.containsKey("Test Long Key"));
        assertEquals(123L, document.get("Test Long Key").getLongValue());
    }

    @Test
    void toJobParametersDocumentEmpty() {
        var document = jobExecutionDocumentMapper.toJobParametersDocument(new JobParameters());

        assertEquals(0, document.keySet().size());
    }

    @Test
    void toJobParameters() {
        var document = new JobParametersDocument();
        document.put("Test String Key", new JobParameterDocument("Test Value", null, null, null, null));
        document.put("Test Long Key", new JobParameterDocument(null, null, 123L, null, null));

        compare(jobParameters, jobExecutionDocumentMapper.toJobParameters(document));
    }

    @Test
    void toJobParameters_empty() {
        assertEquals(0, jobExecutionDocumentMapper.toJobParameters(new JobParametersDocument()).getParameters().size());
    }

    @Data
    static class JobParametersWrapper {
        private JobParametersDocument jobParameters;
    }

    @Test
    void mongoInsertAndFindJobParametersWrapper() {
        var doc = new JobParametersWrapper();
        doc.setJobParameters(jobExecutionDocumentMapper.toJobParametersDocument(jobParameters));
        mongoTemplate.insert(doc, "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParametersWrapper.class, "Test");

        compare(jobParameters, jobExecutionDocumentMapper.toJobParameters(actual.getJobParameters()));
    }

    @Test
    void toJobParameterDocumentString() {
        var doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter("Test Value", false));

        assertEquals("Test Value", doc.getStringValue());
        assertNull(doc.getDateValue());
        assertNull(doc.getLongValue());
        assertNull(doc.getDoubleValue());
        assertFalse(doc.getIdentifying());

        doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter("Test Value", true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void toJobParameterDocumentDate() {
        var doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(testDate, false));

        assertNull(doc.getStringValue());
        assertEquals(testDate, doc.getDateValue());
        assertNull(doc.getLongValue());
        assertNull(doc.getDoubleValue());
        assertFalse(doc.getIdentifying());

        doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(testDate, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void toJobParameterDocumentLong() {
        var doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(123L, false));

        assertNull(doc.getStringValue());
        assertNull(doc.getDateValue());
        assertEquals(123L, doc.getLongValue());
        assertNull(doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(123L, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void toJobParameterDocumentDouble() {
        var doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(3.14, false));

        assertNull(doc.getStringValue());
        assertNull(doc.getDateValue());
        assertNull(doc.getLongValue());
        assertEquals(3.14, doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = jobExecutionDocumentMapper.toJobParameterDocument(new JobParameter(3.14, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void toJobParameterString() {
        var jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument("Test Value", null, null, null, null));

        assertEquals(JobParameter.ParameterType.STRING, jobParameter.getType());
        assertEquals("Test Value", jobParameter.getValue());
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument("Test Value", null, null, null, true));
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument("Test Value", null, null, null, false));
        assertFalse(jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterDate() {
        var jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, testDate, null, null, null));

        assertEquals(JobParameter.ParameterType.DATE, jobParameter.getType());
        assertEquals(testDate, jobParameter.getValue());
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, testDate, null, null, true));
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, testDate, null, null, false));
        assertFalse(jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterLong() {
        var jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, 123L, null, null));

        assertEquals(JobParameter.ParameterType.LONG, jobParameter.getType());
        assertEquals(123L, jobParameter.getValue());
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, 123L, null, true));
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, 123L, null, false));
        assertFalse(jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterDouble() {
        var jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, null, 3.14, null));

        assertEquals(JobParameter.ParameterType.DOUBLE, jobParameter.getType());
        assertEquals(3.14, jobParameter.getValue());
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, null, 3.14, true));
        assertTrue(jobParameter.isIdentifying());

        jobParameter = jobExecutionDocumentMapper.toJobParameter(new JobParameterDocument(null, null, null, 3.14, false));
        assertFalse(jobParameter.isIdentifying());
    }

    @Test
    void mongoInsertAndFindJobParameterString() {
        var expected = new JobParameter("Test Value");

        mongoTemplate.insert(jobExecutionDocumentMapper.toJobParameterDocument(expected), "Test");
        var actual = jobExecutionDocumentMapper.toJobParameter(mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFindJobParameterNotIdentifying() {
        var expected = new JobParameter("Test Value", false);

        mongoTemplate.insert(jobExecutionDocumentMapper.toJobParameterDocument(expected), "Test");
        var actual = jobExecutionDocumentMapper.toJobParameter(mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFindJobParameterDate() {
        var expected = new JobParameter(Date.from(OffsetDateTime.now().toInstant()));

        mongoTemplate.insert(jobExecutionDocumentMapper.toJobParameterDocument(expected), "Test");
        var actual = jobExecutionDocumentMapper.toJobParameter(mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFindJobParameterLong() {
        var expected = new JobParameter(456L);

        mongoTemplate.insert(jobExecutionDocumentMapper.toJobParameterDocument(expected), "Test");
        var actual = jobExecutionDocumentMapper.toJobParameter(mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFindJobParameterDouble() {
        var expected = new JobParameter(12.3);

        mongoTemplate.insert(jobExecutionDocumentMapper.toJobParameterDocument(expected), "Test");
        var actual = jobExecutionDocumentMapper.toJobParameter(mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test"));

        compare(expected, actual);
    }

    @Test
    void serializeContext_deserializeContext() {
        var expected = new ExecutionContext();

        expected.putString("Value 1", "String Value");
        expected.putLong("Value 2", 123L);
        expected.putDouble("Value 3", 1.23);
        expected.putInt("Value 4", 123);
        expected.put("Value 5", new JobParameterDocument(null, null, 987L, null, true));

        var contextString = jobExecutionDocumentMapper.serializeContext(expected);

        var actual = jobExecutionDocumentMapper.deserializeContext(contextString);

        compare(expected, actual);
    }

    @Test
    void serializeContext_Null() {
        assertNull(jobExecutionDocumentMapper.serializeContext(null));
        assertEquals("{\"@class\":\"java.util.HashMap\"}",
                jobExecutionDocumentMapper.serializeContext(new ExecutionContext()));
    }

    @Test
    void serializeContext_IllegalArgumentException() {
        jobExecutionDocumentMapper.setExecutionContextSerializer(new ExecutionContextSerializer() {
            @Override
            public Map<String, Object> deserialize(InputStream inputStream) throws IOException {
                throw new IOException("Test Exception");
            }

            @Override
            public void serialize(Map<String, Object> object, OutputStream outputStream) throws IOException {
                throw new IOException("Test Exception");
            }
        });

        try {
            jobExecutionDocumentMapper.serializeContext(new ExecutionContext());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Could not serialize the execution context", e.getMessage());
            assertEquals("Test Exception", e.getCause().getMessage());
        }
    }

    @Test
    void deserializeContext_null() {
        assertNull(jobExecutionDocumentMapper.deserializeContext(null));
        compare(new ExecutionContext(), jobExecutionDocumentMapper
                .deserializeContext("{\"@class\":\"java.util.HashMap\"}"));
    }

    @Test
    void deserializeContext_IllegalArgumentException() {
        jobExecutionDocumentMapper.setExecutionContextSerializer(new ExecutionContextSerializer() {
            @Override
            public Map<String, Object> deserialize(InputStream inputStream) throws IOException {
                throw new IOException("Test Exception");
            }

            @Override
            public void serialize(Map<String, Object> object, OutputStream outputStream) throws IOException {
                throw new IOException("Test Exception");
            }
        });

        try {
            jobExecutionDocumentMapper.deserializeContext("Test");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Unable to deserialize the execution context", e.getMessage());
            assertEquals("Test Exception", e.getCause().getMessage());
        }
    }

    private void compare(JobInstance expected, JobInstance actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getJobName(), actual.getJobName());
    }

    private void compare(JobExecution expected, JobExecution actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getVersion(), actual.getVersion());
        compare(expected.getJobParameters(), actual.getJobParameters());

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
                compare((StepExecution) expectedSteps[i], (StepExecution) actualSteps[i]);
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

        compare(expected.getExecutionContext(), actual.getExecutionContext());
        assertTrue(CollectionUtils.isEmpty(actual.getFailureExceptions()));
        assertEquals(expected.getJobConfigurationName(), actual.getJobConfigurationName());
    }

    private void compare(StepExecution expected, StepExecution actual) {
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

        compare(expected.getExecutionContext(), actual.getExecutionContext());

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

    private void compare(JobParameters expected, JobParameters actual) {
        assertEquals(expected.getParameters().size(), actual.getParameters().size());
        for (var key : expected.getParameters().keySet()) {
            compare(expected.getParameters().get(key), actual.getParameters().get(key));
        }
    }

    private void compare(JobParameter expected, JobParameter actual) {
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getValue(), actual.getValue());
        assertEquals(expected.isIdentifying(), actual.isIdentifying());
    }

    private void compare(ExecutionContext expected, ExecutionContext actual) {
        if (expected == null) {
            assertTrue(actual == null || actual.entrySet().size() == 0);
            return;
        }
        for (var entry : expected.entrySet()) {
            assertTrue(actual.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), actual.get(entry.getKey()));
        }
    }

}