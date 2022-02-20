package io.github.wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility Class for converting objects of type {@link ExecutionContext} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExecutionContextConverter {

    private static Jackson2ExecutionContextStringSerializer serializer = new Jackson2ExecutionContextStringSerializer();

    public static String serializeContext(ExecutionContext ctx) {
        if (ctx == null) {
            return "";
        }
        Map<String, Object> m = new HashMap<>();
        for (Map.Entry<String, Object> me : ctx.entrySet()) {
            m.put(me.getKey(), me.getValue());
        }

        try {
            var out = new ByteArrayOutputStream();
            serializer.serialize(m, out);
            return out.toString("UTF-8");
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Could not serialize the execution context", ioe);
        }
    }

    public static ExecutionContext deserializeContext(String serializedContext) {
        if (!StringUtils.hasLength(serializedContext)) {
            return null;
        }
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(serializedContext.getBytes("UTF-8"));
            return new ExecutionContext(serializer.deserialize(in));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
        }
    }

    /**
     * Convert the source object of type {@link ExecutionContext} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link ExecutionContext} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     * @deprecated please use serializeContext instead
     */
    @Deprecated(since = "1.0.3", forRemoval = true)
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
     * @deprecated please use deserializeContext instead
     */
    @Deprecated(since = "1.0.3", forRemoval = true)
    public static ExecutionContext convert(@Nullable final Document source) {
        if (source == null) {
            return new ExecutionContext();
        }
        return new ExecutionContext(source);
    }
}
