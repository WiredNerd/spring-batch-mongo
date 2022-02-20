package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.data.mongodb.core.query.Query;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobParametersDocumentTest extends MongoDBContainerConfig {

    public static void compare(JobParameters expected, JobParameters actual) {
        assertEquals(expected.getParameters().size(), actual.getParameters().size());
        for (var key : expected.getParameters().keySet()) {
            JobParameterDocumentTest.compare(expected.getParameters().get(key), actual.getParameters().get(key));
        }
    }

    JobParameters jobParameters;

    @BeforeEach
    void setupJobParameters() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L));

        jobParameters = new JobParameters(paramMap);
    }

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(JobParametersDocument.from(jobParameters));

        assertTrue(docJsonString.contains("\"Test String Key\":{\"STRING\":\"Test Value\"}"), docJsonString);
        assertTrue(docJsonString.contains("\"Test Long Key\":{\"LONG\":123}"), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParametersDocument.class);

        compare(resultDoc.toJobParameters(), jobParameters);
    }

    @Test
    void jacksonObjectMapper_empty() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        jobParameters = new JobParameters();

        var docJsonString = jacksonObjectMapper.writeValueAsString(JobParametersDocument.from(jobParameters));

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobParametersDocument.class);

        compare(resultDoc.toJobParameters(), jobParameters);
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter()
                .convertToMongoType(JobParametersDocument.from(jobParameters));

        assertEquals(2, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParametersDocument.class, docBson);

        compare(resultDoc.toJobParameters(), jobParameters);
    }

    @Test
    void mongoTemplateConverter_nulls() {
        jobParameters = new JobParameters();

        var docBson = (org.bson.Document) mongoTemplate.getConverter()
                .convertToMongoType(JobParametersDocument.from(jobParameters));

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobParametersDocument.class, docBson);

        compare(resultDoc.toJobParameters(), jobParameters);
    }

    @Test
    void fromJobParameters() {
        var document = JobParametersDocument.from(jobParameters);

        assertEquals(2, document.keySet().size());
        assertTrue(document.containsKey("Test String Key"));
        assertEquals("Test Value", document.get("Test String Key").getStringValue());
        assertTrue(document.containsKey("Test Long Key"));
        assertEquals(123L, document.get("Test Long Key").getLongValue());
    }

    @Test
    void fromJobParameters_empty() {
        var document = JobParametersDocument.from(new JobParameters());

        assertEquals(0, document.keySet().size());
    }

    @Test
    void toJobParameters() {
        var document = new JobParametersDocument();
        document.put("Test String Key", new JobParameterDocument("Test Value", null, null, null, null));
        document.put("Test Long Key", new JobParameterDocument(null, null, 123L, null, null));

        compare(jobParameters, document.toJobParameters());
    }

    @Test
    void toJobParameters_empty() {
        assertEquals(0, new JobParametersDocument().toJobParameters().getParameters().size());
    }

    @Data
    class JobParametersWrapper {
        private JobParametersDocument jobParameters;
    }

    @Test
    void mongoInsertAndFind_String() {
        var doc = new JobParametersWrapper();
        doc.setJobParameters(JobParametersDocument.from(jobParameters));
        mongoTemplate.insert(doc, "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParametersWrapper.class, "Test");

        compare(jobParameters, actual.getJobParameters().toJobParameters());
    }
}