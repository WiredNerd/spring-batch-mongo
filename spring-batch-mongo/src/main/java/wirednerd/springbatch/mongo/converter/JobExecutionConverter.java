package wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.util.Assert;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobExecutionConverter {

    private static final String JOB_EXECUTION_ID = "jobExecutionId";
    private static final String JOB_PARAMETERS = "jobParameters";
    private static final String JOB_INSTANCE_ID = "jobInstanceId";
    private static final String JOB_NAME = "jobName";
    private static final String JOB_KEY = "jobKey";
    private static final String STEP_EXECUTIONS = "stepExecutions";
    private static final String STATUS = "status";
    private static final String START_TIME = "startTime";
    private static final String CREATE_TIME = "createTime";
    private static final String END_TIME = "endTime";
    private static final String LAST_UPDATED = "lastUpdated";
    private static final String EXIT_CODE = "exitCode";
    private static final String EXIT_DESCRIPTION = "exitDescription";
    private static final String EXECUTION_CONTEXT = "executionContext";
    private static final String JOB_CONFIGURATION_NAME = "jobConfigurationName";

    private static JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    /**
     * Convert the source object of type {@link JobExecution} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobExecution} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(JobExecution source) {
        Document document = new Document();

        Assert.notNull(source.getId(), "jobExecutionId must not be null");
        Assert.notNull(source.getJobInstance(), "jobInstance must not be null");
        Assert.notNull(source.getJobInstance().getId(), "jobInstanceId must not be null");
        Assert.notNull(source.getStatus(), "status must not be null");
        Assert.notNull(source.getExitStatus(), "exitStatus must not be null");

        document.put(JOB_EXECUTION_ID, source.getId());
        document.put(JOB_PARAMETERS, JobParametersConverter.convert(source.getJobParameters()));

        document.put(JOB_INSTANCE_ID, source.getJobInstance().getId());
        document.put(JOB_NAME, source.getJobInstance().getJobName());
        document.put(JOB_KEY, jobKeyGenerator.generateKey(source.getJobParameters()));

        // TODO: stepExecutions

        document.put(STATUS, source.getStatus().toString());
        document.put(START_TIME, source.getStartTime());
        document.put(CREATE_TIME, source.getCreateTime());
        document.put(END_TIME, source.getEndTime());
        document.put(LAST_UPDATED, source.getLastUpdated());
        document.put(EXIT_CODE, source.getExitStatus().getExitCode());
        document.put(EXIT_DESCRIPTION, source.getExitStatus().getExitDescription());

        // TODO: executionContext

        document.put(JOB_CONFIGURATION_NAME, source.getJobConfigurationName());

        return document;
    }

    /**
     * Convert the source object of type {@link Document} to target type {@link JobExecution}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobExecution} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static JobExecution convert(Document source) {
        JobInstance job = null;
        if (source.get(JOB_NAME) != null) {
            job = new JobInstance(source.getLong(JOB_INSTANCE_ID), source.getString(JOB_NAME));
        }
        var jobExecutionId = source.getLong(JOB_EXECUTION_ID);
        var jobParameters = JobParametersConverter.convert(source.get(JOB_PARAMETERS, Document.class));
        var jobConfigurationName = source.getString(JOB_CONFIGURATION_NAME);

        var jobExecution = new JobExecution(job, jobExecutionId, jobParameters, jobConfigurationName);

        // TODO: stepExecutions

        var status = source.getString(STATUS);
        jobExecution.setStatus(status == null ? null : BatchStatus.valueOf(status));

        jobExecution.setStartTime(source.getDate(START_TIME));
        jobExecution.setCreateTime(source.getDate(CREATE_TIME));
        jobExecution.setEndTime(source.getDate(END_TIME));
        jobExecution.setLastUpdated(source.getDate(LAST_UPDATED));
        jobExecution.setExitStatus(new ExitStatus(source.getString(EXIT_CODE), source.getString(EXIT_DESCRIPTION)));

        // TODO: executionContext

        return jobExecution;
    }
}
