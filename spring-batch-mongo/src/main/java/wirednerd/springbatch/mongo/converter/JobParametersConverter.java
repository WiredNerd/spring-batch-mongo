package wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobParametersConverter {

    /**
     * Convert the source object of type {@link Document} to target type {@link JobParameters}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobParameters} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static JobParameters convert(Document source) {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        if (!CollectionUtils.isEmpty(source)) {
            source.forEach((key, value) -> {
                if (value instanceof Document) {
                    paramMap.put(key, JobParameterConverter.convert((Document) value));
                }
            });
        }

        return new JobParameters(paramMap);
    }

    /**
     * Convert the source object of type {@link JobParameters} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobParameters} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(JobParameters source) {
        Document document = new Document();
        source.getParameters().forEach((key, value) -> document.put(key, JobParameterConverter.convert(value)));
        return document;
    }
}
