package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class JobParameterDocumentTest extends MongoDBContainerConfig {

    private final Date testDate = Date.from(OffsetDateTime.of(2022, 2, 19, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());

    private JobParameterDocument jobParameterDocument = new JobParameterDocument("Test Value",
            testDate, 123L, 3.14, true);

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(jobParameterDocument);

        assertTrue(docJsonString.contains("\"STRING\":\"Test Value\""), docJsonString);
        assertTrue(docJsonString.contains("\"DATE\":\"2022-02-19T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"LONG\":123"), docJsonString);
        assertTrue(docJsonString.contains("\"DOUBLE\":3.14"), docJsonString);
        assertTrue(docJsonString.contains("\"identifying\":true"), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParameterDocument.class);

        assertEquals(jobParameterDocument.getStringValue(), resultDoc.getStringValue());
        assertEquals(testDate, resultDoc.getDateValue());
        assertEquals(jobParameterDocument.getLongValue(), resultDoc.getLongValue());
        assertEquals(jobParameterDocument.getDoubleValue(), resultDoc.getDoubleValue());
        assertEquals(jobParameterDocument.getIdentifying(), resultDoc.getIdentifying());
    }

    @Test
    void jacksonObjectMapper_nulls() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        jobParameterDocument = new JobParameterDocument(null, null, null, null, null);
        var docJsonString = jacksonObjectMapper.writeValueAsString(jobParameterDocument);

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParameterDocument.class);

        assertNull(resultDoc.getStringValue());
        assertNull(resultDoc.getDateValue());
        assertNull(resultDoc.getLongValue());
        assertNull(resultDoc.getDoubleValue());
        assertNull(resultDoc.getIdentifying());
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobParameterDocument);

        assertEquals(jobParameterDocument.getStringValue(), docBson.getString("STRING"));
        assertEquals(testDate, docBson.getDate("DATE"));
        assertEquals(jobParameterDocument.getLongValue(), docBson.getLong("LONG"));
        assertEquals(jobParameterDocument.getDoubleValue(), docBson.getDouble("DOUBLE"));
        assertEquals(jobParameterDocument.getIdentifying(), docBson.getBoolean("identifying"));
        assertEquals(5, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParameterDocument.class, docBson);

        assertEquals(jobParameterDocument.getStringValue(), resultDoc.getStringValue());
        assertEquals(testDate, resultDoc.getDateValue());
        assertEquals(jobParameterDocument.getLongValue(), resultDoc.getLongValue());
        assertEquals(jobParameterDocument.getDoubleValue(), resultDoc.getDoubleValue());
        assertEquals(jobParameterDocument.getIdentifying(), resultDoc.getIdentifying());
    }

    @Test
    void mongoTemplateConverter_nulls() {
        jobParameterDocument = new JobParameterDocument(null, null, null, null, null);
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobParameterDocument);

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParameterDocument.class, docBson);

        assertNull(resultDoc.getStringValue());
        assertNull(resultDoc.getDateValue());
        assertNull(resultDoc.getLongValue());
        assertNull(resultDoc.getDoubleValue());
        assertNull(resultDoc.getIdentifying());
    }
}