package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.JobParameters;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.LinkedHashMap;

/**
 * <p>This class enables data from a {@link JobParameters} object to be converted to Document formats.</p>
 * Supported converters: {@link ObjectMapper}, {@link MongoTemplate}
 *
 * @author Peter Busch
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobParametersDocument extends LinkedHashMap<String, JobParameterDocument> {

    /**
     * This constructor is intended to be used by deserialization tools.
     */
    public JobParametersDocument() {//NOPMD
        super();
    }
}
