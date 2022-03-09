package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringReader;
import java.io.StringWriter;

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

    @Test
    void jaxb() throws JAXBException {
        var context = JAXBContext.newInstance(JobInstanceDocument.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(jobInstanceDocument, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<jobInstance>"), xmlString);
        assertTrue(xmlString.contains("</jobInstance>"), xmlString);
        assertTrue(xmlString.contains("<jobInstanceId>1</jobInstanceId>"), xmlString);
        assertTrue(xmlString.contains("<jobName>NAME</jobName>"), xmlString);
        assertTrue(xmlString.contains("<jobKey>KEY</jobKey>"), xmlString);

        var resultDoc = (JobInstanceDocument) unmarshaller.unmarshal(new StringReader(xmlString));

        assertEquals(jobInstanceDocument, resultDoc);
    }

    @Test
    void jaxb_nulls() throws JAXBException {
        jobInstanceDocument = new JobInstanceDocument();

        var context = JAXBContext.newInstance(JobInstanceDocument.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(jobInstanceDocument, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<jobInstance/>"), xmlString);

        var resultDoc = (JobInstanceDocument) unmarshaller.unmarshal(new StringReader(xmlString));

        assertEquals(jobInstanceDocument, resultDoc);
    }
}