package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.StringReader;
import java.io.StringWriter;

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

    @Test
    void jaxb() throws JAXBException {
        var wrapper = new Wrapper();
        wrapper.setJobParametersDocument(jobParametersDocument);

        var context = JAXBContext.newInstance(Wrapper.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(wrapper, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<wrapper><jobParametersDocument>"), xmlString);
        assertTrue(xmlString.contains("</jobParametersDocument></wrapper>"), xmlString);
        assertTrue(xmlString.contains("<entry><key>Test String Key</key><value><STRING>Test Value</STRING></value></entry>"), xmlString);
        assertTrue(xmlString.contains("<entry><key>Test Long Key</key><value><LONG>123</LONG></value></entry>"), xmlString);

        var wrapperOut = (Wrapper) unmarshaller.unmarshal(new StringReader(xmlString));
        var resultDoc = wrapperOut.getJobParametersDocument();

        compare(resultDoc, jobParametersDocument);
    }

    @Test
    void jaxb_nulls() throws JAXBException {
        jobParametersDocument = new JobParametersDocument();

        var wrapper = new Wrapper();
        wrapper.setJobParametersDocument(jobParametersDocument);

        var context = JAXBContext.newInstance(Wrapper.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(wrapper, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<jobParametersDocument/>"), xmlString);

        var wrapperOut = (Wrapper) unmarshaller.unmarshal(new StringReader(xmlString));
        var resultDoc = wrapperOut.getJobParametersDocument();

        compare(resultDoc, jobParametersDocument);
    }

    private static void compare(JobParametersDocument expected, JobParametersDocument actual) {
        assertEquals(expected.size(), actual.size());
        for (var key : expected.keySet()) {
            assertEquals(expected.get(key), actual.get(key));
        }
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    @Data
    static class Wrapper {
        private JobParametersDocument jobParametersDocument;
    }
}