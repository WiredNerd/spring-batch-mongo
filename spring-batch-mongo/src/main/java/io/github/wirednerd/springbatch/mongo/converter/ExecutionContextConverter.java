package io.github.wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Utility Class for converting objects of type {@link ExecutionContext} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExecutionContextConverter {

    /**
     * Convert the source object of type {@link ExecutionContext} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link ExecutionContext} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(@Nullable final ExecutionContext source) {
        Document document = new Document();
        if (source == null || CollectionUtils.isEmpty(source.entrySet())) {
            return document;
        }
        source.entrySet().forEach(entry -> document.put(entry.getKey(), entry.getValue()));
        return document;
    }

    /**
     * Convert the source object of type {@link Document} to target type {@link ExecutionContext}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link ExecutionContext} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static ExecutionContext convert(@Nullable final Document source) {
        if (source == null) {
            return new ExecutionContext();
        }
        return new ExecutionContext(source);
    }
}
