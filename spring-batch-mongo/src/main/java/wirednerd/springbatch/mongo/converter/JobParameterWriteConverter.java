package wirednerd.springbatch.mongo.converter;

import org.bson.Document;
import org.springframework.batch.core.JobParameter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

@WritingConverter
public class JobParameterWriteConverter implements Converter<JobParameter, Document> {

    private static final String IDENTIFYING = "identifying";
    private static final String STRING = "STRING";
    private static final String DATE = "DATE";
    private static final String LONG = "LONG";
    private static final String DOUBLE = "DOUBLE";

    /**
     * Convert the source object of type {@code S} to target type {@code T}.
     *
     * @param source the source object to convert, which must be an instance of {@code S} (never {@code null})
     * @return the converted object, which must be an instance of {@code T} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    @Override
    public Document convert(JobParameter source) {
        Document document = new Document();

        switch (source.getType()) {
            case STRING:
                document.put(STRING, source.getValue());
                break;
            case DATE:
                document.put(DATE, source.getValue());
                break;
            case LONG:
                document.put(LONG, source.getValue());
                break;
            case DOUBLE:
                document.put(DOUBLE, source.getValue());
                break;
        }

        if (!source.isIdentifying()) {
            document.put(IDENTIFYING, source.isIdentifying());
        }
        return document;
    }
}