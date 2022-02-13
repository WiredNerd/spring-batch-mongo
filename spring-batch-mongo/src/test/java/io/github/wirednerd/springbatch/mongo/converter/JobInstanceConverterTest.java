package io.github.wirednerd.springbatch.mongo.converter;

import io.github.wirednerd.springbatch.mongo.MongoDBContainerConfig;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class JobInstanceConverterTest extends MongoDBContainerConfig {

    private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    @Test
    void mongoInsertAndFind_jobInstance() {
        var expected = JobExecutionConverterTest.buildJobExecution();

        mongoTemplate.insert(JobInstanceConverter.convert(expected.getJobInstance(), expected.getJobParameters()), "Test");

        var document = mongoTemplate.findOne(new Query(), Document.class, "Test");
        assertEquals(jobKeyGenerator.generateKey(expected.getJobParameters()), document.getString("jobKey"));

        var actual = JobInstanceConverter.convert(document);

        assertEquals(expected.getJobInstance().getInstanceId(), actual.getInstanceId());
        assertEquals(expected.getJobInstance().getJobName(), actual.getJobName());
    }

    @Test
    void converters_JobInstanceToDocument_null_jobInstance() {
        try {
            JobInstanceConverter.convert(null, new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstance must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobInstanceToDocument_null_jobInstanceId() {
        var expected = new JobInstance(null, "Example Job");

        try {
            JobInstanceConverter.convert(expected, new JobParameters());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobInstanceId must not be null", e.getMessage());
        }
    }

    @Test
    void converters_JobInstanceToDocument_null_jobParameters() {
        var expected = new JobInstance(1L, "Example Job");

        try {
            JobInstanceConverter.convert(expected, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobParameters must not be null", e.getMessage());
        }
    }

}