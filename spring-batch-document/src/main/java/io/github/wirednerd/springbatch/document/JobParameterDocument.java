package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * <p>This class enables data from a {@link JobParameter} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@Getter
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("SameNameButDifferent")
public class JobParameterDocument {

    @JsonProperty(STRING)
    @Field(STRING)
    private final String stringValue;

    @JsonProperty(DATE)
    @Field(DATE)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    private final Date dateValue;

    @JsonProperty(LONG)
    @Field(LONG)
    private final Long longValue;

    @JsonProperty(DOUBLE)
    @Field(DOUBLE)
    private final Double doubleValue;

    @JsonProperty(IDENTIFYING)
    @Field(IDENTIFYING)
    private final Boolean identifying;

    /**
     * This constructor is intended to be used by deserialization tools.
     *
     * @param stringValue value of "STRING" field
     * @param dateValue   value of "DATE" field
     * @param longValue   value of "LONG" field
     * @param doubleValue value of "DOUBLE" field
     * @param identifying value of "identifying" field
     */
    @JsonCreator
    @PersistenceConstructor
    public JobParameterDocument(
            @JsonProperty(STRING)
            @Value("#root.STRING") String stringValue,
            @JsonProperty(DATE)
            @Value("#root.DATE") Date dateValue,
            @JsonProperty(LONG)
            @Value("#root.LONG") Long longValue,
            @JsonProperty(DOUBLE)
            @Value("#root.DOUBLE") Double doubleValue,
            @JsonProperty(IDENTIFYING)
            @Value("#root.identifying") Boolean identifying) {
        this.identifying = identifying;
        this.stringValue = stringValue;
        this.dateValue = dateValue;
        this.longValue = longValue;
        this.doubleValue = doubleValue;
    }
}
