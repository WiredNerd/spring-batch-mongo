package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobParametersDocumentTest extends MongoDBContainerConfig {

    private JobParametersDocument jobParametersDocument;

    @BeforeEach
    void setupJobParameters() {
        jobParametersDocument = new JobParametersDocument();
        jobParametersDocument.put("Test String Key", new JobParameterDocument("Test Value", null, null, null, null));
        jobParametersDocument.put("Test Long Key", new JobParameterDocument(null, null, 123L, null, null));
    }

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(jobParametersDocument);

        assertTrue(docJsonString.contains("\"Test String Key\":{\"STRING\":\"Test Value\"}"), docJsonString);
        assertTrue(docJsonString.contains("\"Test Long Key\":{\"LONG\":123}"), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParametersDocument.class);

        compare(resultDoc, jobParametersDocument);
    }

    @Test
    void jacksonObjectMapper_empty() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        jobParametersDocument = new JobParametersDocument();

        var docJsonString = jacksonObjectMapper.writeValueAsString(jobParametersDocument);

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParametersDocument.class);

        compare(resultDoc, jobParametersDocument);
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter()
                .convertToMongoType(jobParametersDocument);

        assertEquals(2, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParametersDocument.class, docBson);

        compare(resultDoc, jobParametersDocument);
    }

    @Test
    void mongoTemplateConverter_nulls() {
        jobParametersDocument = new JobParametersDocument();

        var docBson = (org.bson.Document) mongoTemplate.getConverter()
                .convertToMongoType(jobParametersDocument);

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParametersDocument.class, docBson);

        compare(resultDoc, jobParametersDocument);
    }

    private static void compare(JobParametersDocument expected, JobParametersDocument actual) {
        assertEquals(expected.size(), actual.size());
        for (var key : expected.keySet()) {
            assertEquals(expected.get(key), actual.get(key));
        }
    }
}