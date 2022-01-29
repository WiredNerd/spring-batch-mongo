package wirednerd.springbatch.mongo.converter;

import org.bson.Document;
import org.springframework.batch.core.JobParameters;
import org.springframework.core.convert.converter.Converter;

public class JobParametersWriteConverter implements Converter<JobParameters, Document> {

    private final JobParameterWriteConverter jobParameterConverter = new JobParameterWriteConverter();

    /**
     * Convert the source object of type {@link JobParameters} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobParameters} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    @Override
    public Document convert(JobParameters source) {
        Document document = new Document();
        source.getParameters().forEach((key, value) -> document.put(key, jobParameterConverter.convert(value)));
        return document;
    }
}
