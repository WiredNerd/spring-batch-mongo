package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.batch.core.StepExecution;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * <p>This class enables data from a {@link StepExecution} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement(name = "stepExecution")
@XmlAccessorType(XmlAccessType.FIELD)
@SuppressWarnings("SameNameButDifferent")
public class StepExecutionDocument {

    @JsonProperty(STEP_EXECUTION_ID)
    @Field(STEP_EXECUTION_ID)
    @XmlElement(name = STEP_EXECUTION_ID)
    private Long stepExecutionId;

    @JsonProperty(STEP_NAME)
    @Field(STEP_NAME)
    @XmlElement(name = STEP_NAME)
    private String stepName;

    @JsonProperty(READ_COUNT)
    @Field(READ_COUNT)
    @XmlElement(name = READ_COUNT)
    private Integer readCount;

    @JsonProperty(WRITE_COUNT)
    @Field(WRITE_COUNT)
    @XmlElement(name = WRITE_COUNT)
    private Integer writeCount;

    @JsonProperty(COMMIT_COUNT)
    @Field(COMMIT_COUNT)
    @XmlElement(name = COMMIT_COUNT)
    private Integer commitCount;

    @JsonProperty(ROLLBACK_COUNT)
    @Field(ROLLBACK_COUNT)
    @XmlElement(name = ROLLBACK_COUNT)
    private Integer rollbackCount;

    @JsonProperty(READ_SKIP_COUNT)
    @Field(READ_SKIP_COUNT)
    @XmlElement(name = READ_SKIP_COUNT)
    private Integer readSkipCount;

    @JsonProperty(PROCESS_SKIP_COUNT)
    @Field(PROCESS_SKIP_COUNT)
    @XmlElement(name = PROCESS_SKIP_COUNT)
    private Integer processSkipCount;

    @JsonProperty(WRITE_SKIP_COUNT)
    @Field(WRITE_SKIP_COUNT)
    @XmlElement(name = WRITE_SKIP_COUNT)
    private Integer writeSkipCount;

    @JsonProperty(FILTER_COUNT)
    @Field(FILTER_COUNT)
    @XmlElement(name = FILTER_COUNT)
    private Integer filterCount;

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
}
