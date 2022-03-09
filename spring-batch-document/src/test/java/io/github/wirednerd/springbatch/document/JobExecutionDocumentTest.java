package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.util.Lists;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobExecutionDocumentTest extends MongoDBContainerConfig {

    private final Date testDateCreate = Date.from(OffsetDateTime.of(2022, 2, 18, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());
    private final Date testDateStart = Date.from(OffsetDateTime.of(2022, 2, 19, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());
    private final Date testDateEnd = Date.from(OffsetDateTime.of(2022, 2, 20, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());
    private final Date testDateUpdate = Date.from(OffsetDateTime.of(2022, 2, 21, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());

    private JobExecutionDocument jobExecutionDocument;
    private StepExecutionDocument stepExecutionDocument;
    private JobParametersDocument jobParametersDocument;

    @BeforeEach
    void setupData() {
        stepExecutionDocument = new StepExecutionDocument();

        stepExecutionDocument.setStepExecutionId(1L);
        stepExecutionDocument.setStepName("Name");
        stepExecutionDocument.setReadCount(2);
        stepExecutionDocument.setWriteCount(3);
        stepExecutionDocument.setCommitCount(4);
        stepExecutionDocument.setRollbackCount(5);
        stepExecutionDocument.setReadSkipCount(6);
        stepExecutionDocument.setProcessSkipCount(7);
        stepExecutionDocument.setWriteSkipCount(8);
        stepExecutionDocument.setFilterCount(9);
        stepExecutionDocument.setStatus("Status");
        stepExecutionDocument.setStartTime(testDateStart);
        stepExecutionDocument.setEndTime(testDateEnd);
        stepExecutionDocument.setLastUpdated(testDateUpdate);
        stepExecutionDocument.setExitCode("Exit");
        stepExecutionDocument.setExitDescription("Desc");
        stepExecutionDocument.setExecutionContext("ctx");

        jobParametersDocument = new JobParametersDocument();
        jobParametersDocument.put("Test String Key", new JobParameterDocument("Test Value", null, null, null, null));
        jobParametersDocument.put("Test Long Key", new JobParameterDocument(null, null, 123L, null, null));

        jobExecutionDocument = new JobExecutionDocument();

        jobExecutionDocument.setJobExecutionId(2L);
        jobExecutionDocument.setVersion(3);
        jobExecutionDocument.setJobParameters(jobParametersDocument);
        jobExecutionDocument.setJobInstanceId(3L);
        jobExecutionDocument.setJobName("Name");
        jobExecutionDocument.setJobKey("Key");
        jobExecutionDocument.setStepExecutions(Lists.newArrayList(stepExecutionDocument));
        jobExecutionDocument.setStatus("StatusVal");
        jobExecutionDocument.setStartTime(testDateStart);
        jobExecutionDocument.setCreateTime(testDateCreate);
        jobExecutionDocument.setEndTime(testDateEnd);
        jobExecutionDocument.setLastUpdated(testDateUpdate);
        jobExecutionDocument.setExitCode("Exit");
        jobExecutionDocument.setExitDescription("Desc");
        jobExecutionDocument.setExecutionContext("Context");
        jobExecutionDocument.setJobConfigurationName("Config");
    }

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(jobExecutionDocument);

        assertTrue(docJsonString.contains("\"jobExecutionId\":2"), docJsonString);
        assertTrue(docJsonString.contains("\"version\":3"), docJsonString);
        assertTrue(docJsonString.contains("\"jobParameters\":{"), docJsonString);
        assertTrue(docJsonString.contains("\"jobInstanceId\":3"), docJsonString);
        assertTrue(docJsonString.contains("\"jobName\":\"Name\""), docJsonString);
        assertTrue(docJsonString.contains("\"jobKey\":\"Key\""), docJsonString);
        assertTrue(docJsonString.contains("\"stepExecutions\":[{"), docJsonString);
        assertTrue(docJsonString.contains("\"status\":\"StatusVal\""), docJsonString);
        assertTrue(docJsonString.contains("\"startTime\":\"2022-02-19T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"createTime\":\"2022-02-18T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"endTime\":\"2022-02-20T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"lastUpdated\":\"2022-02-21T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"exitCode\":\"Exit\""), docJsonString);
        assertTrue(docJsonString.contains("\"exitDescription\":\"Desc\""), docJsonString);
        assertTrue(docJsonString.contains("\"executionContext\":\"Context\""), docJsonString);
        assertTrue(docJsonString.contains("\"jobConfigurationName\":\"Config\""), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobExecutionDocument.class);

        assertEquals(jobExecutionDocument, resultDoc);
    }

    @Test
    void jacksonObjectMapper_nulls() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        jobExecutionDocument = new JobExecutionDocument();
        var docJsonString = jacksonObjectMapper.writeValueAsString(jobExecutionDocument);

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, JobExecutionDocument.class);

        assertEquals(jobExecutionDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobExecutionDocument);

        assertEquals(jobExecutionDocument.getJobExecutionId(), docBson.getLong("jobExecutionId"));
        assertEquals(jobExecutionDocument.getVersion(), docBson.getInteger("version"));
        assertEquals(jobExecutionDocument.getJobParameters().size(), docBson.get("jobParameters", Document.class).size());
        assertEquals(jobExecutionDocument.getJobInstanceId(), docBson.getLong("jobInstanceId"));
        assertEquals(jobExecutionDocument.getJobName(), docBson.getString("jobName"));
        assertEquals(jobExecutionDocument.getJobKey(), docBson.getString("jobKey"));
        assertEquals(jobExecutionDocument.getStepExecutions().size(), docBson.getList("stepExecutions", Document.class).size());
        assertEquals(jobExecutionDocument.getStatus(), docBson.getString("status"));
        assertEquals(jobExecutionDocument.getStartTime(), docBson.getDate("startTime"));
        assertEquals(jobExecutionDocument.getCreateTime(), docBson.getDate("createTime"));
        assertEquals(jobExecutionDocument.getEndTime(), docBson.getDate("endTime"));
        assertEquals(jobExecutionDocument.getLastUpdated(), docBson.getDate("lastUpdated"));
        assertEquals(jobExecutionDocument.getExitCode(), docBson.getString("exitCode"));
        assertEquals(jobExecutionDocument.getExitDescription(), docBson.getString("exitDescription"));
        assertEquals(jobExecutionDocument.getExecutionContext(), docBson.getString("executionContext"));
        assertEquals(jobExecutionDocument.getJobConfigurationName(), docBson.getString("jobConfigurationName"));
        assertEquals(16, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobExecutionDocument.class, docBson);

        assertEquals(jobExecutionDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter_nulls() {
        jobExecutionDocument = new JobExecutionDocument();
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(jobExecutionDocument);

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(JobExecutionDocument.class, docBson);

        assertEquals(jobExecutionDocument, resultDoc);
    }

    @Test
    void jaxb() throws JAXBException {
        var context = JAXBContext.newInstance(JobExecutionDocument.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(jobExecutionDocument, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<jobExecution>"), xmlString);
        assertTrue(xmlString.contains("</jobExecution>"), xmlString);
        assertTrue(xmlString.contains("<jobExecutionId>2</jobExecutionId>"), xmlString);
        assertTrue(xmlString.contains("<version>3</version>"), xmlString);

        assertTrue(xmlString.contains("<jobParameters>"), xmlString);
        assertTrue(xmlString.contains("</jobParameters>"), xmlString);

        assertTrue(xmlString.contains("<jobInstanceId>3</jobInstanceId>"), xmlString);
        assertTrue(xmlString.contains("<jobName>Name</jobName>"), xmlString);
        assertTrue(xmlString.contains("<jobKey>Key</jobKey>"), xmlString);

        assertTrue(xmlString.contains("<stepExecutions>"), xmlString);
        assertTrue(xmlString.contains("</stepExecutions>"), xmlString);

        assertTrue(xmlString.contains("<status>StatusVal</status>"), xmlString);
        assertTrue(xmlString.contains("<startTime>2022-02-19T01:02:04.005+0000</startTime>"), xmlString);
        assertTrue(xmlString.contains("<createTime>2022-02-18T01:02:04.005+0000</createTime>"), xmlString);
        assertTrue(xmlString.contains("<endTime>2022-02-20T01:02:04.005+0000</endTime>"), xmlString);
        assertTrue(xmlString.contains("<lastUpdated>2022-02-21T01:02:04.005+0000</lastUpdated>"), xmlString);
        assertTrue(xmlString.contains("<exitCode>Exit</exitCode>"), xmlString);
        assertTrue(xmlString.contains("<exitDescription>Desc</exitDescription>"), xmlString);
        assertTrue(xmlString.contains("<executionContext>Context</executionContext>"), xmlString);
        assertTrue(xmlString.contains("<jobConfigurationName>Config</jobConfigurationName>"), xmlString);

        var resultDoc = (JobExecutionDocument) unmarshaller.unmarshal(new StringReader(xmlString));

        assertEquals(jobExecutionDocument, resultDoc);
    }

    @Test
    void jaxb_nulls() throws JAXBException {
        jobExecutionDocument = new JobExecutionDocument();

        var context = JAXBContext.newInstance(JobExecutionDocument.class);

        var marshaller = context.createMarshaller();
        var unmarshaller = context.createUnmarshaller();

        var stringWriter = new StringWriter();

        marshaller.marshal(jobExecutionDocument, stringWriter);
        String xmlString = stringWriter.toString();

        assertTrue(xmlString.contains("<jobExecution/>"), xmlString);

        var resultDoc = (JobExecutionDocument) unmarshaller.unmarshal(new StringReader(xmlString));

        assertEquals(jobExecutionDocument, resultDoc);
    }
}