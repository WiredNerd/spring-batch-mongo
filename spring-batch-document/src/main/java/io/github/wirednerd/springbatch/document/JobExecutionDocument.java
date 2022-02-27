package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.batch.core.JobExecution;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * <p>This class enables data from a {@link JobExecution} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobExecutionDocument {

    @JsonProperty(JOB_EXECUTION_ID)
    @Field(JOB_EXECUTION_ID)
    private Long jobExecutionId;

    @JsonProperty(VERSION)
    @Field(VERSION)
    private Integer version;

    @JsonProperty(JOB_PARAMETERS)
    @Field(JOB_PARAMETERS)
    private JobParametersDocument jobParameters;

    @JsonProperty(JOB_INSTANCE_ID)
    @Field(JOB_INSTANCE_ID)
    private Long jobInstanceId;

    @JsonProperty(JOB_NAME)
    @Field(JOB_NAME)
    private String jobName;

    @JsonProperty(JOB_KEY)
    @Field(JOB_KEY)
    private String jobKey;

    @JsonProperty(STEP_EXECUTIONS)
    @Field(STEP_EXECUTIONS)
    private List<StepExecutionDocument> stepExecutions;

    @JsonProperty(STATUS)
    @Field(STATUS)
    private String status;

    @JsonProperty(START_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(START_TIME)
    private Date startTime;

    @JsonProperty(CREATE_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(CREATE_TIME)
    private Date createTime;

    @JsonProperty(END_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(END_TIME)
    private Date endTime;

    @JsonProperty(LAST_UPDATED)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(LAST_UPDATED)
    private Date lastUpdated;

    @JsonProperty(EXIT_CODE)
    @Field(EXIT_CODE)
    private String exitCode;

    @JsonProperty(EXIT_DESCRIPTION)
    @Field(EXIT_DESCRIPTION)
    private String exitDescription;

    @JsonProperty(EXECUTION_CONTEXT)
    @Field(EXECUTION_CONTEXT)
    private String executionContext;

    @JsonProperty(JOB_CONFIGURATION_NAME)
    @Field(JOB_CONFIGURATION_NAME)
    private String jobConfigurationName;
}
