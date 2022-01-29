package wirednerd.springbatch.mongo.converter;

import org.bson.Document;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.core.convert.converter.Converter;

import java.util.LinkedHashMap;

public class JobParametersReadConverter implements Converter<Document, JobParameters> {

    private final JobParameterReadConverter jobParameterConverter = new JobParameterReadConverter();

    /**
     * Convert the source object of type {@link Document} to target type {@link JobParameters}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobParameters} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    @Override
    public JobParameters convert(Document source) {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        source.forEach((key, value) -> {
            if (value instanceof Document) {
                paramMap.put(key, jobParameterConverter.convert((Document) value));
            }
        });

        return new JobParameters(paramMap);
    }
}
