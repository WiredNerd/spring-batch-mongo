package io.github.wirednerd.springbatch.document;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class converts Spring Batch Job Execution Objects to Document objects.
 *
 * @author Peter Busch
 */
@Setter
@NoArgsConstructor
@SuppressWarnings("SameNameButDifferent")
public class JobExecutionDocumentMapper {

    public static final String ISO_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    // JobExecutionDocument
    public static final String JOB_EXECUTION_ID = "jobExecutionId";
    public static final String VERSION = "version";
    public static final String JOB_PARAMETERS = "jobParameters";
    public static final String JOB_INSTANCE_ID = "jobInstanceId";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_KEY = "jobKey";
    public static final String STEP_EXECUTIONS = "stepExecutions";
    public static final String STATUS = "status";
    public static final String START_TIME = "startTime";
    public static final String CREATE_TIME = "createTime";
    public static final String END_TIME = "endTime";
    public static final String LAST_UPDATED = "lastUpdated";
    public static final String EXIT_CODE = "exitCode";
    public static final String EXIT_DESCRIPTION = "exitDescription";
    public static final String EXECUTION_CONTEXT = "executionContext";
    public static final String JOB_CONFIGURATION_NAME = "jobConfigurationName";

    // JobParameter
    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String LONG = "LONG";
    public static final String DOUBLE = "DOUBLE";
    public static final String IDENTIFYING = "identifying";

    // StepExecution
    public static final String STEP_EXECUTION_ID = "stepExecutionId";
    public static final String STEP_NAME = "stepName";
    public static final String READ_COUNT = "readCount";
    public static final String WRITE_COUNT = "writeCount";
    public static final String COMMIT_COUNT = "commitCount";
    public static final String ROLLBACK_COUNT = "rollbackCount";
    public static final String READ_SKIP_COUNT = "readSkipCount";
    public static final String PROCESS_SKIP_COUNT = "processSkipCount";
    public static final String WRITE_SKIP_COUNT = "writeSkipCount";
    public static final String FILTER_COUNT = "filterCount";


    private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();
    private ExecutionContextSerializer executionContextSerializer = new Jackson2ExecutionContextStringSerializer();
    private Charset executionContextCharset = StandardCharsets.UTF_8;  // Charset used by Jackson2ExecutionContextStringSerializer

    /**
     * Convert the source object of type {@link JobExecution} to target type {@link JobExecutionDocument}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobExecution} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobExecutionDocument} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public JobExecutionDocument toJobExecutionDocument(JobExecution source) {
        var document = new JobExecutionDocument();

        Assert.notNull(source.getId(), "jobExecutionId must not be null");
        Assert.notNull(source.getJobInstance(), "jobInstance must not be null");
        Assert.notNull(source.getJobInstance().getId(), "jobInstanceId must not be null");
        Assert.notNull(source.getStatus(), "status must not be null");
        Assert.notNull(source.getExitStatus(), "exitStatus must not be null");

        document.setJobExecutionId(source.getId());
        document.setVersion(source.getVersion());
        document.setJobParameters(toJobParametersDocument(source.getJobParameters()));

        document.setJobInstanceId(source.getJobInstance().getId());
        document.setJobName(source.getJobInstance().getJobName());
        document.setJobKey(jobKeyGenerator.generateKey(source.getJobParameters()));

        var stepDocuments = source.getStepExecutions().stream()
                .map(step -> toStepExecutionDocument(step)).collect(Collectors.toList());
        document.setStepExecutions(stepDocuments);

        document.setStatus(source.getStatus().toString());
        document.setStartTime(source.getStartTime());
        document.setCreateTime(source.getCreateTime());
        document.setEndTime(source.getEndTime());
        document.setLastUpdated(source.getLastUpdated());
        document.setExitCode(source.getExitStatus().getExitCode());
        document.setExitDescription(source.getExitStatus().getExitDescription());
        document.setExecutionContext(serializeContext(source.getExecutionContext()));
        document.setJobConfigurationName(source.getJobConfigurationName());

        return document;
    }

    /**
     * Convert the source object of type {@link JobExecutionDocument} to target type {@link JobExecution}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobExecutionDocument} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobExecution} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public JobExecution toJobExecution(JobExecutionDocument source) {
        JobInstance job = new JobInstance(source.getJobInstanceId(), source.getJobName());

        var jobExecutionId = source.getJobExecutionId();
        var jobParameters = toJobParameters(source.getJobParameters());
        var jobConfigurationName = source.getJobConfigurationName();

        var jobExecution = new JobExecution(job, jobExecutionId, jobParameters, jobConfigurationName);

        if (!CollectionUtils.isEmpty(source.getStepExecutions())) {
            source.getStepExecutions().forEach(doc -> toStepExecution(doc, jobExecution));
        }

        var status = source.getStatus();
        jobExecution.setStatus(status == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(status));

        jobExecution.setVersion(source.getVersion());
        jobExecution.setStartTime(source.getStartTime());
        jobExecution.setCreateTime(source.getCreateTime());
        jobExecution.setEndTime(source.getEndTime());
        jobExecution.setLastUpdated(source.getLastUpdated());
        jobExecution.setExitStatus(new ExitStatus(source.getExitCode(), source.getExitDescription()));
        jobExecution.setExecutionContext(deserializeContext(source.getExecutionContext()));

        return jobExecution;
    }

    /**
     * Convert the source object of type {@link JobInstance} to target type {@link JobExecutionDocument}.
     * Result should be readable as a JobExecution with only jobInstanceId, jobName, and jobKey.
     *
     * @param source        the source object to convert, which must be an instance of {@link JobInstance} (never {@code null})
     * @param jobParameters are used for generating the jobKey, which must be an instance of {@link JobParameters} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobExecutionDocument} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public JobExecutionDocument toJobExecutionDocument(JobInstance source, JobParameters jobParameters) {

        Assert.notNull(source, "jobInstance must not be null");
        Assert.notNull(source.getId(), "jobInstanceId must not be null");
        Assert.notNull(jobParameters, "jobParameters must not be null");

        var document = new JobExecutionDocument();

        document.setJobInstanceId(source.getId());
        document.setJobName(source.getJobName());
        document.setJobKey(jobKeyGenerator.generateKey(jobParameters));

        return document;
    }

    /**
     * Convert the source object of type {@link JobInstance} to target type {@link JobInstanceDocument}.
     * Result should be readable as a JobExecution with only jobInstanceId, jobName, and jobKey.
     *
     * @param source        the source object to convert, which must be an instance of {@link JobInstance} (never {@code null})
     * @param jobParameters are used for generating the jobKey, which must be an instance of {@link JobParameters} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobInstanceDocument} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public JobInstanceDocument toJobInstanceDocument(JobInstance source, JobParameters jobParameters) {

        Assert.notNull(source, "jobInstance must not be null");
        Assert.notNull(source.getId(), "jobInstanceId must not be null");
        Assert.notNull(jobParameters, "jobParameters must not be null");

        var document = new JobInstanceDocument();

        document.setJobInstanceId(source.getId());
        document.setJobName(source.getJobName());
        document.setJobKey(jobKeyGenerator.generateKey(jobParameters));

        return document;
    }

    /**
     * Convert the source object of type {@link JobInstanceDocument} to target type {@link JobInstance}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobInstanceDocument} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobInstance} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public JobInstance toJobInstance(JobInstanceDocument source) {
        return new JobInstance(source.getJobInstanceId(), source.getJobName());
    }

    /**
     * <p>Construct a new {@link StepExecutionDocument} using data from a {@link StepExecution} object.</p>
     *
     * @param source the source object to convert, which must be an instance of {@link StepExecution} (never {@code null})
     * @return the converted object, which must be an instance of {@link StepExecutionDocument} (never {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public StepExecutionDocument toStepExecutionDocument(StepExecution source) {

        Assert.notNull(source.getId(), "stepExecutionId must not be null");
        Assert.notNull(source.getStatus(), "status must not be null");
        Assert.notNull(source.getExitStatus(), "exitStatus must not be null");

        var document = new StepExecutionDocument();

        document.setStepExecutionId(source.getId());
        document.setStepName(source.getStepName());
        document.setStatus(source.getStatus().toString());
        document.setReadCount(source.getReadCount());
        document.setWriteCount(source.getWriteCount());
        document.setCommitCount(source.getCommitCount());
        document.setRollbackCount(source.getRollbackCount());
        document.setReadSkipCount(source.getReadSkipCount());
        document.setProcessSkipCount(source.getProcessSkipCount());
        document.setWriteSkipCount(source.getWriteSkipCount());
        document.setStartTime(source.getStartTime());
        document.setEndTime(source.getEndTime());
        document.setLastUpdated(source.getLastUpdated());
        document.setExecutionContext(serializeContext(source.getExecutionContext()));
        document.setExitCode(source.getExitStatus().getExitCode());
        document.setExitDescription(source.getExitStatus().getExitDescription());
        document.setFilterCount(source.getFilterCount());

        return document;
    }

    /**
     * <p>Construct a new {@link StepExecution} using data from a {@link StepExecutionDocument} object.</p>
     *
     * @param source       the source object to convert, which must be an instance of {@link StepExecutionDocument} (never {@code null})
     * @param jobExecution is the instance of {@link JobExecution} referenced from the final {@link StepExecution}
     * @return the converted object, which must be an instance of {@link StepExecution} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public StepExecution toStepExecution(StepExecutionDocument source, JobExecution jobExecution) {

        var stepExecution = new StepExecution(source.getStepName(), jobExecution, source.getStepExecutionId());

        stepExecution.setStatus(source.getStatus() == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(source.getStatus()));

        stepExecution.setReadCount(ifNull(source.getReadCount(), 0));
        stepExecution.setWriteCount(ifNull(source.getWriteCount(), 0));
        stepExecution.setCommitCount(ifNull(source.getCommitCount(), 0));
        stepExecution.setRollbackCount(ifNull(source.getRollbackCount(), 0));
        stepExecution.setReadSkipCount(ifNull(source.getReadSkipCount(), 0));
        stepExecution.setProcessSkipCount(ifNull(source.getProcessSkipCount(), 0));
        stepExecution.setWriteSkipCount(ifNull(source.getWriteSkipCount(), 0));
        stepExecution.setStartTime(source.getStartTime());
        stepExecution.setEndTime(source.getEndTime());
        stepExecution.setLastUpdated(source.getLastUpdated());
        stepExecution.setExecutionContext(deserializeContext(source.getExecutionContext()));
        stepExecution.setExitStatus(new ExitStatus(source.getExitCode(), source.getExitDescription()));
        stepExecution.setFilterCount(ifNull(source.getFilterCount(), 0));
        return stepExecution;
    }

    private int ifNull(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * Convert a map of (String, {@link JobParameter}) to a map of (String, {@link JobParameterDocument})
     *
     * @param jobParameters {@link JobParameters} to convert. never {@code null}
     * @return a new {@link JobParametersDocument}
     */
    public JobParametersDocument toJobParametersDocument(JobParameters jobParameters) {
        var document = new JobParametersDocument();
        jobParameters.getParameters().forEach((key, value) -> document.put(key, toJobParameterDocument(value)));
        return document;
    }

    /**
     * Convert this map of (String, {@link JobParameterDocument}) to a {@link JobParameters} object
     *
     * @param document {@link JobParametersDocument} to convert. never {@code null}
     * @return {@link JobParameters}
     */
    public JobParameters toJobParameters(JobParametersDocument document) {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        if (!CollectionUtils.isEmpty(document)) {
            document.forEach((key, value) -> paramMap.put(key, toJobParameter(value)));
        }
        return new JobParameters(paramMap);
    }

    /**
     * <p>Construct a new {@link JobParameterDocument} using data from a {@link JobParameter} object.</p>
     *
     * <p>The {@code identifying} field is populated as {@code false} only when the {@code jobParameter.isIdentifying()} returns false.
     * Otherwise, {@code identifying} field is {@code null}</p>
     *
     * @param jobParameter {@link JobParameter} to convert. never {@code null}
     * @return a new {@link JobParameterDocument}
     */
    public JobParameterDocument toJobParameterDocument(JobParameter jobParameter) {
        Boolean identifying = null;
        if (!jobParameter.isIdentifying()) {
            identifying = false;
        }

        switch (jobParameter.getType()) {
            case STRING:
                return new JobParameterDocument((String) jobParameter.getValue(), null, null, null, identifying);
            case DATE:
                return new JobParameterDocument(null, (Date) jobParameter.getValue(), null, null, identifying);
            case LONG:
                return new JobParameterDocument(null, null, (Long) jobParameter.getValue(), null, identifying);
            case DOUBLE:
                return new JobParameterDocument(null, null, null, (Double) jobParameter.getValue(), identifying);
        }
        throw new IllegalArgumentException("JobParameter.ParameterType not recognized");
    }

    /**
     * <p>Construct a new {@link JobParameterDocument} using data from a {@link JobParameter} object.</p>
     *
     * <p>{@code identifying} defaults to true when {@code null}</p>
     *
     * @param document {@link JobParameterDocument} to convert. never {@code null}
     * @return {@link JobParameter}
     * @throws IllegalArgumentException if this object does not contain any parameter data
     */
    public JobParameter toJobParameter(JobParameterDocument document) {
        boolean identifyingOut = document.getIdentifying() == null || document.getIdentifying();

        if (document.getStringValue() != null) {
            return new JobParameter(document.getStringValue(), identifyingOut);
        }
        if (document.getDateValue() != null) {
            return new JobParameter(document.getDateValue(), identifyingOut);
        }
        if (document.getLongValue() != null) {
            return new JobParameter(document.getLongValue(), identifyingOut);
        }
        if (document.getDoubleValue() != null) {
            return new JobParameter(document.getDoubleValue(), identifyingOut);
        }

        throw new IllegalArgumentException("Job Parameter must include STRING, DATE, LONG, or DOUBLE field");
    }

    /**
     * Serialize {@link ExecutionContext} to a String
     *
     * @param executionContext {@link ExecutionContext}
     * @return String
     */
    public String serializeContext(ExecutionContext executionContext) {
        if (executionContext == null) {
            return null;
        }
        Map<String, Object> contextMap = new HashMap<>(); //NOPMD
        executionContext.entrySet().forEach(entry -> contextMap.put(entry.getKey(), entry.getValue()));

        try {
            var out = new ByteArrayOutputStream();
            executionContextSerializer.serialize(contextMap, out);
            return out.toString(executionContextCharset);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Could not serialize the execution context", ioe);
        }
    }

    /**
     * Deserialize a String to an {@link ExecutionContext}
     *
     * @param serializedContext Serialized String of an {@link ExecutionContext}
     * @return {@link ExecutionContext}
     */
    public ExecutionContext deserializeContext(String serializedContext) {
        if (!StringUtils.hasLength(serializedContext)) {
            return null;
        }
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedContext.getBytes(executionContextCharset));
            return new ExecutionContext(executionContextSerializer.deserialize(byteArrayInputStream));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
        }
    }
}
