package wirednerd;

import org.bson.Document;
import org.springframework.batch.core.JobParameter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

@ReadingConverter
public class JobParameterReadConverter implements Converter<Document, JobParameter> {

    private static final String IDENTIFYING = "identifying";
    private static final String STRING = "STRING";
    private static final String DATE = "DATE";
    private static final String LONG = "LONG";
    private static final String DOUBLE = "DOUBLE";
    private static final String ILLEGAL_ARGUMENT = "Job Parameter must include STRING, DATE, LONG, or DOUBLE field";

    /**
     * Convert the source object of type {@code S} to target type {@code T}.
     *
     * @param document the source object to convert, which must be an instance of {@code S} (never {@code null})
     * @return the converted object, which must be an instance of {@code T} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    @Override
    public JobParameter convert(Document document) {
        boolean identifying = document.getBoolean(IDENTIFYING, true);

        if (document.containsKey(STRING)) {
            return new JobParameter(document.getString(STRING), identifying);
        }
        if (document.containsKey(DATE)) {
            return new JobParameter(document.getDate(DATE), identifying);
        }
        if (document.containsKey(LONG)) {
            return new JobParameter(document.getLong(LONG), identifying);
        }
        if (document.containsKey(DOUBLE)) {
            return new JobParameter(document.getDouble(DOUBLE), identifying);
        }

        throw new IllegalArgumentException(ILLEGAL_ARGUMENT);
    }
}
