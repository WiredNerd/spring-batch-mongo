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
public class JobParameterDocument {

    public static final String ISO_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String LONG = "LONG";
    public static final String DOUBLE = "DOUBLE";
    public static final String IDENTIFYING = "identifying";

    @JsonProperty(STRING)
    @Field(STRING)
    private String stringValue;

    @JsonProperty(DATE)
    @Field(DATE)
    @JsonFormat(pattern = ISO_DATE_PATTERN)
    private Date dateValue;

    @JsonProperty(LONG)
    @Field(LONG)
    private Long longValue;

    @JsonProperty(DOUBLE)
    @Field(DOUBLE)
    private Double doubleValue;

    @JsonProperty(IDENTIFYING)
    @Field(IDENTIFYING)
    private Boolean identifying;

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

    /**
     * Construct a new {@link JobParameterDocument} using data from a {@link JobParameter} object
     *
     * <p>The {@code identifying} field is populated as {@code false} only when the {@code jobParameter.isIdentifying()} returns false.
     * Otherwise, {@code identifying} field is {@code null}</p>
     *
     * @param jobParameter {@link JobParameter} to convert. never {@code null}
     * @return a new {@link JobParameterDocument}
     */
    public static JobParameterDocument from(JobParameter jobParameter) {
        Boolean identifying = false;
        if (jobParameter.isIdentifying()) {
            identifying = null;
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
     * <p>Converts this {@link JobParameterDocument} into a {@link JobParameter} object.</p>
     *
     * <p>{@code identifying} defaults to true when {@code null}</p>
     *
     * @return {@link JobParameter}
     * @throws IllegalArgumentException if this object does not contain any parameter data
     */
    public JobParameter toJobParameter() {
        boolean identifyingOut = identifying == null ? true : identifying;

        if (stringValue != null) {
            return new JobParameter(stringValue, identifyingOut);
        }
        if (dateValue != null) {
            return new JobParameter(dateValue, identifyingOut);
        }
        if (longValue != null) {
            return new JobParameter(longValue, identifyingOut);
        }
        if (doubleValue != null) {
            return new JobParameter(doubleValue, identifyingOut);
        }

        throw new IllegalArgumentException("Job Parameter must include STRING, DATE, LONG, or DOUBLE field");
    }
}
