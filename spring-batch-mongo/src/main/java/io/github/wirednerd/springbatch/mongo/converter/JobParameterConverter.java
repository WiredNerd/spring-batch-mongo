package io.github.wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.JobParameter;

import static io.github.wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

/**
 * Utility Class for converting objects of type {@link JobParameter} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobParameterConverter {

    /**
     * Convert the source object of type {@link Document} to target type {@link JobParameter}.
     *
     * @param document the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobParameter} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static JobParameter convert(final Document document) {
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

        throw new IllegalArgumentException("Job Parameter must include STRING, DATE, LONG, or DOUBLE field");
    }

    /**
     * Convert the source object of type {@link JobParameter} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link JobParameter} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(final JobParameter source) {
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
