package wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

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

        document.put(JOB_EXECUTION_ID, source.getId());
        document.put(VERSION, source.getVersion());
        document.put(JOB_PARAMETERS, JobParametersConverter.convert(source.getJobParameters()));

        document.put(JOB_INSTANCE_ID, source.getJobInstance().getId());
        document.put(JOB_NAME, source.getJobInstance().getJobName());
        document.put(JOB_KEY, jobKeyGenerator.generateKey(source.getJobParameters()));

        var stepDocuments = source.getStepExecutions().stream()
                .map(step -> StepExecutionConverter.convert(step)).collect(Collectors.toList());
        document.put(STEP_EXECUTIONS, stepDocuments);

        document.put(STATUS, source.getStatus().toString());
        document.put(START_TIME, source.getStartTime());
        document.put(CREATE_TIME, source.getCreateTime());
        document.put(END_TIME, source.getEndTime());
        document.put(LAST_UPDATED, source.getLastUpdated());
        document.put(EXIT_CODE, source.getExitStatus().getExitCode());
        document.put(EXIT_DESCRIPTION, source.getExitStatus().getExitDescription());
        document.put(EXECUTION_CONTEXT, ExecutionContextConverter.convert(source.getExecutionContext()));
        document.put(JOB_CONFIGURATION_NAME, source.getJobConfigurationName());

        return document;
    }

    /**
     * Convert the source object of type {@link JobInstance} to target type {@link Document}.
     * Result should be readable as a JobExecution with only jobInstanceId, jobName, and jobKey.
     *
     * @param source        the source object to convert, which must be an instance of {@link JobInstance} (never {@code null})
     * @param jobParameters are used for generating the jobKey, which is part of the stored Job Instance data.
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(final JobInstance source, final JobParameters jobParameters) {
        Document document = new Document();

        Assert.notNull(source, "jobInstance must not be null");
        Assert.notNull(source.getId(), "jobInstanceId must not be null");
        Assert.notNull(source.getJobName(), "status must not be null");
        Assert.notNull(jobParameters, "jobParameters must not be null");

        document.put(JOB_INSTANCE_ID, source.getId());
        document.put(JOB_NAME, source.getJobName());
        document.put(JOB_KEY, jobKeyGenerator.generateKey(jobParameters));

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
        JobInstance job = new JobInstance(source.getLong(JOB_INSTANCE_ID), source.getString(JOB_NAME));

        var jobExecutionId = source.getLong(JOB_EXECUTION_ID);
        var jobParameters = JobParametersConverter.convert(source.get(JOB_PARAMETERS, Document.class));
        var jobConfigurationName = source.getString(JOB_CONFIGURATION_NAME);

        var jobExecution = new JobExecution(job, jobExecutionId, jobParameters, jobConfigurationName);

        var steps = source.getList(STEP_EXECUTIONS, Document.class, new ArrayList<>()).stream()
                .map(doc -> StepExecutionConverter.convert(doc, jobExecution)).collect(Collectors.toList());
        jobExecution.addStepExecutions(steps);

        var status = source.getString(STATUS);
        jobExecution.setStatus(status == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(status));

        jobExecution.setVersion(source.getInteger(VERSION));
        jobExecution.setStartTime(source.getDate(START_TIME));
        jobExecution.setCreateTime(source.getDate(CREATE_TIME));
        jobExecution.setEndTime(source.getDate(END_TIME));
        jobExecution.setLastUpdated(source.getDate(LAST_UPDATED));
        jobExecution.setExitStatus(new ExitStatus(source.getString(EXIT_CODE), source.getString(EXIT_DESCRIPTION)));
        jobExecution.setExecutionContext(ExecutionContextConverter.convert(source.get(EXECUTION_CONTEXT, Document.class)));
        return jobExecution;
    }
}
