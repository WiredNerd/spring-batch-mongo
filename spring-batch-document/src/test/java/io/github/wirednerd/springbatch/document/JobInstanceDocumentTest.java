package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobInstanceDocumentTest extends MongoDBContainerConfig {

    private JobInstanceDocument jobInstanceDocument;

    @BeforeEach
    void setupDat() {
        jobInstanceDocument = new JobInstanceDocument();

        jobInstanceDocument.setJobInstanceId(1L);
        jobInstanceDocument.setJobName("NAME");
        jobInstanceDocument.setJobKey("KEY");
    }

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(jobInstanceDocument);

        assertTrue(docJsonString.contains("\"jobInstanceId\":1"), docJsonString);
        assertTrue(docJsonString.contains("\"jobName\":\"NAME\""), docJsonString);
        assertTrue(docJsonString.contains("\"jobKey\":\"KEY\""), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobInstanceDocument.class);

        assertEquals(jobInstanceDocument, resultDoc);
    }

    @Test
    void jacksonObjectMapper_nulls() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        jobInstanceDocument = new JobInstanceDocument();
        var docJsonString = jacksonObjectMapper.writeValueAsString(jobInstanceDocument);

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobInstanceDocument.class);

        assertEquals(jobInstanceDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobInstanceDocument);

        assertEquals(jobInstanceDocument.getJobInstanceId(), docBson.getLong("jobInstanceId"));
        assertEquals(jobInstanceDocument.getJobName(), docBson.getString("jobName"));
        assertEquals(jobInstanceDocument.getJobKey(), docBson.getString("jobKey"));
        assertEquals(3, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobInstanceDocument.class, docBson);

        assertEquals(jobInstanceDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter_nulls() {
        jobInstanceDocument = new JobInstanceDocument();
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobInstanceDocument);

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobInstanceDocument.class, docBson);

        assertEquals(jobInstanceDocument, resultDoc);
    }
}