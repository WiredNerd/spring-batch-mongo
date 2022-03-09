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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.List;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * <p>This class enables data from a {@link JobExecution} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@SuppressWarnings("SameNameButDifferent")
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement(name = "jobExecution")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobExecutionDocument {

    @JsonProperty(JOB_EXECUTION_ID)
    @Field(JOB_EXECUTION_ID)
    @XmlElement(name = JOB_EXECUTION_ID)
    private Long jobExecutionId;

    @JsonProperty(VERSION)
    @Field(VERSION)
    @XmlElement(name = VERSION)
    private Integer version;

    @JsonProperty(JOB_PARAMETERS)
    @Field(JOB_PARAMETERS)
    @XmlElement(name = JOB_PARAMETERS)
    private JobParametersDocument jobParameters;

    @JsonProperty(JOB_INSTANCE_ID)
    @Field(JOB_INSTANCE_ID)
    @XmlElement(name = JOB_INSTANCE_ID)
    private Long jobInstanceId;

    @JsonProperty(JOB_NAME)
    @Field(JOB_NAME)
    @XmlElement(name = JOB_NAME)
    private String jobName;

    @JsonProperty(JOB_KEY)
    @Field(JOB_KEY)
    @XmlElement(name = JOB_KEY)
    private String jobKey;

    @JsonProperty(STEP_EXECUTIONS)
    @Field(STEP_EXECUTIONS)
    @XmlElement(name = STEP_EXECUTIONS)
    private List<StepExecutionDocument> stepExecutions;

    @JsonProperty(STATUS)
    @Field(STATUS)
    @XmlElement(name = STATUS)
    private String status;

    @JsonProperty(START_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(START_TIME)
    @XmlElement(name = START_TIME)
    @XmlJavaTypeAdapter(DateXmlAdapter.class)
    private Date startTime;

    @JsonProperty(CREATE_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(CREATE_TIME)
    @XmlElement(name = CREATE_TIME)
    @XmlJavaTypeAdapter(DateXmlAdapter.class)
    private Date createTime;

    @JsonProperty(END_TIME)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(END_TIME)
    @XmlElement(name = END_TIME)
    @XmlJavaTypeAdapter(DateXmlAdapter.class)
    private Date endTime;

    @JsonProperty(LAST_UPDATED)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    @Field(LAST_UPDATED)
    @XmlElement(name = LAST_UPDATED)
    @XmlJavaTypeAdapter(DateXmlAdapter.class)
    private Date lastUpdated;

    @JsonProperty(EXIT_CODE)
    @Field(EXIT_CODE)
    @XmlElement(name = EXIT_CODE)
    private String exitCode;

    @JsonProperty(EXIT_DESCRIPTION)
    @Field(EXIT_DESCRIPTION)
    @XmlElement(name = EXIT_DESCRIPTION)
    private String exitDescription;

    @JsonProperty(EXECUTION_CONTEXT)
    @Field(EXECUTION_CONTEXT)
    @XmlElement(name = EXECUTION_CONTEXT)
    private String executionContext;

    @JsonProperty(JOB_CONFIGURATION_NAME)
    @Field(JOB_CONFIGURATION_NAME)
    @XmlElement(name = JOB_CONFIGURATION_NAME)
    private String jobConfigurationName;
}
