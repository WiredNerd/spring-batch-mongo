package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobParametersDocument extends LinkedHashMap<String, JobParameterDocument> {

    /**
     * This constructor is intended to be used by deserialization tools.
     */
    public JobParametersDocument() {
        super();
    }

    /**
     * Convert a map of String -> {@link JobParameter} to a map of String ->  {@link JobParameterDocument}
     *
     * @param jobParameters {@link JobParameters} to convert. never {@code null}
     * @return a new {@link JobParametersDocument}
     */
    public static JobParametersDocument from(JobParameters jobParameters) {
        var document = new JobParametersDocument();
        jobParameters.getParameters().forEach((key, value) -> document.put(key, JobParameterDocument.from(value)));
        return document;
    }

    /**
     * Convert this map of String ->  {@link JobParameterDocument} to a {@link JobParameters} object
     *
     * @return {@link JobParameters}
     */
    public JobParameters toJobParameters() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        if (!CollectionUtils.isEmpty(this)) {
            this.forEach((key, value) -> {
                paramMap.put(key, value.toJobParameter());
            });
        }
        return new JobParameters(paramMap);
    }
}
