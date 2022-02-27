package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.springframework.batch.core.JobInstance;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * <p>This class enables data from a {@link JobInstance} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobInstanceDocument {

    @JsonProperty(JOB_INSTANCE_ID)
    @Field(JOB_INSTANCE_ID)
    private Long jobInstanceId;

    @JsonProperty(JOB_NAME)
    @Field(JOB_NAME)
    private String jobName;

    @JsonProperty(JOB_KEY)
    @Field(JOB_KEY)
    private String jobKey;
}
