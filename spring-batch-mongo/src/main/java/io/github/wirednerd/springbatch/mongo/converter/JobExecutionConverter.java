package io.github.wirednerd.springbatch.mongo.converter;

import io.github.wirednerd.springbatch.mongo.MongodbRepositoryConstants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility Class for converting objects of type {@link JobExecution} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobExecutionConverter {

    private static JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    /**
     * Convert the source object of type {@link JobExecution} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobExecution} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(final JobExecution source) {
        Document document = new Document();

        Assert.notNull(source.getId(), "jobExecutionId must not be null");
        Assert.notNull(source.getJobInstance(), "jobInstance must not be null");
        Assert.notNull(source.getJobInstance().getId(), "jobInstanceId must not be null");
        Assert.notNull(source.getStatus(), "status must not be null");
        Assert.notNull(source.getExitStatus(), "exitStatus must not be null");

        document.put(MongodbRepositoryConstants.JOB_EXECUTION_ID, source.getId());
        document.put(MongodbRepositoryConstants.VERSION, source.getVersion());
        document.put(MongodbRepositoryConstants.JOB_PARAMETERS, JobParametersConverter.convert(source.getJobParameters()));

        document.put(MongodbRepositoryConstants.JOB_INSTANCE_ID, source.getJobInstance().getId());
        document.put(MongodbRepositoryConstants.JOB_NAME, source.getJobInstance().getJobName());
        document.put(MongodbRepositoryConstants.JOB_KEY, jobKeyGenerator.generateKey(source.getJobParameters()));

        var stepDocuments = source.getStepExecutions().stream()
                .map(step -> StepExecutionConverter.convert(step)).collect(Collectors.toList());
        document.put(MongodbRepositoryConstants.STEP_EXECUTIONS, stepDocuments);

        document.put(MongodbRepositoryConstants.STATUS, source.getStatus().toString());
        document.put(MongodbRepositoryConstants.START_TIME, source.getStartTime());
        document.put(MongodbRepositoryConstants.CREATE_TIME, source.getCreateTime());
        document.put(MongodbRepositoryConstants.END_TIME, source.getEndTime());
        document.put(MongodbRepositoryConstants.LAST_UPDATED, source.getLastUpdated());
        document.put(MongodbRepositoryConstants.EXIT_CODE, source.getExitStatus().getExitCode());
        document.put(MongodbRepositoryConstants.EXIT_DESCRIPTION, source.getExitStatus().getExitDescription());
        document.put(MongodbRepositoryConstants.EXECUTION_CONTEXT, ExecutionContextConverter.serializeContext(source.getExecutionContext()));
        document.put(MongodbRepositoryConstants.JOB_CONFIGURATION_NAME, source.getJobConfigurationName());

        return document;
    }

    /**
     * Convert the source object of type {@link Document} to target type {@link JobExecution}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobExecution} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static JobExecution convert(final Document source) {
        JobInstance job = new JobInstance(source.getLong(MongodbRepositoryConstants.JOB_INSTANCE_ID), source.getString(MongodbRepositoryConstants.JOB_NAME));

        var jobExecutionId = source.getLong(MongodbRepositoryConstants.JOB_EXECUTION_ID);
        var jobParameters = JobParametersConverter.convert(source.get(MongodbRepositoryConstants.JOB_PARAMETERS, Document.class));
        var jobConfigurationName = source.getString(MongodbRepositoryConstants.JOB_CONFIGURATION_NAME);

        var jobExecution = new JobExecution(job, jobExecutionId, jobParameters, jobConfigurationName);

        source.getList(MongodbRepositoryConstants.STEP_EXECUTIONS, Document.class, new ArrayList<>())
                .forEach(doc -> StepExecutionConverter.convert(doc, jobExecution));

        var status = source.getString(MongodbRepositoryConstants.STATUS);
        jobExecution.setStatus(status == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(status));

        jobExecution.setVersion(source.getInteger(MongodbRepositoryConstants.VERSION));
        jobExecution.setStartTime(source.getDate(MongodbRepositoryConstants.START_TIME));
        jobExecution.setCreateTime(source.getDate(MongodbRepositoryConstants.CREATE_TIME));
        jobExecution.setEndTime(source.getDate(MongodbRepositoryConstants.END_TIME));
        jobExecution.setLastUpdated(source.getDate(MongodbRepositoryConstants.LAST_UPDATED));
        jobExecution.setExitStatus(new ExitStatus(source.getString(MongodbRepositoryConstants.EXIT_CODE), source.getString(MongodbRepositoryConstants.EXIT_DESCRIPTION)));
        jobExecution.setExecutionContext(ExecutionContextConverter.deserializeContext(source.getString(MongodbRepositoryConstants.EXECUTION_CONTEXT)));
        return jobExecution;
    }
}
