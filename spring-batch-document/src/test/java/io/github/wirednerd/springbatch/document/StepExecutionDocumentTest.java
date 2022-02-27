package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepExecutionDocumentTest extends MongoDBContainerConfig {

    private final Date testDateStart = Date.from(OffsetDateTime.of(2022, 2, 19, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());
    private final Date testDateEnd = Date.from(OffsetDateTime.of(2022, 2, 20, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());
    private final Date testDateUpdate = Date.from(OffsetDateTime.of(2022, 2, 21, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());

    private StepExecutionDocument stepExecutionDocument;

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
    }

    @Test
    void jacksonObjectMapper() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        var docJsonString = jacksonObjectMapper.writeValueAsString(stepExecutionDocument);

        assertTrue(docJsonString.contains("\"stepExecutionId\":1"), docJsonString);
        assertTrue(docJsonString.contains("\"stepName\":\"Name\""), docJsonString);
        assertTrue(docJsonString.contains("\"readCount\":2"), docJsonString);
        assertTrue(docJsonString.contains("\"writeCount\":3"), docJsonString);
        assertTrue(docJsonString.contains("\"commitCount\":4"), docJsonString);
        assertTrue(docJsonString.contains("\"rollbackCount\":5"), docJsonString);
        assertTrue(docJsonString.contains("\"readSkipCount\":6"), docJsonString);
        assertTrue(docJsonString.contains("\"processSkipCount\":7"), docJsonString);
        assertTrue(docJsonString.contains("\"writeSkipCount\":8"), docJsonString);
        assertTrue(docJsonString.contains("\"filterCount\":9"), docJsonString);
        assertTrue(docJsonString.contains("\"status\":\"Status\""), docJsonString);
        assertTrue(docJsonString.contains("\"startTime\":\"2022-02-19T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"endTime\":\"2022-02-20T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"lastUpdated\":\"2022-02-21T01:02:04.005+0000\""), docJsonString);
        assertTrue(docJsonString.contains("\"exitCode\":\"Exit\""), docJsonString);
        assertTrue(docJsonString.contains("\"exitDescription\":\"Desc\""), docJsonString);
        assertTrue(docJsonString.contains("\"executionContext\":\"ctx\""), docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, StepExecutionDocument.class);

        assertEquals(stepExecutionDocument, resultDoc);
    }

    @Test
    void jacksonObjectMapper_nulls() throws JsonProcessingException {
        ObjectMapper jacksonObjectMapper = new ObjectMapper();

        stepExecutionDocument = new StepExecutionDocument();
        var docJsonString = jacksonObjectMapper.writeValueAsString(stepExecutionDocument);

        assertEquals("{}", docJsonString);

        var resultDoc = jacksonObjectMapper.readValue(docJsonString, StepExecutionDocument.class);

        assertEquals(stepExecutionDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter() {
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(stepExecutionDocument);

        assertEquals(stepExecutionDocument.getStepExecutionId(), docBson.getLong("stepExecutionId"));
        assertEquals(stepExecutionDocument.getStepName(), docBson.getString("stepName"));
        assertEquals(stepExecutionDocument.getReadCount(), docBson.getInteger("readCount"));
        assertEquals(stepExecutionDocument.getWriteCount(), docBson.getInteger("writeCount"));
        assertEquals(stepExecutionDocument.getCommitCount(), docBson.getInteger("commitCount"));
        assertEquals(stepExecutionDocument.getRollbackCount(), docBson.getInteger("rollbackCount"));
        assertEquals(stepExecutionDocument.getReadSkipCount(), docBson.getInteger("readSkipCount"));
        assertEquals(stepExecutionDocument.getProcessSkipCount(), docBson.getInteger("processSkipCount"));
        assertEquals(stepExecutionDocument.getWriteSkipCount(), docBson.getInteger("writeSkipCount"));
        assertEquals(stepExecutionDocument.getFilterCount(), docBson.getInteger("filterCount"));
        assertEquals(stepExecutionDocument.getStatus(), docBson.getString("status"));
        assertEquals(stepExecutionDocument.getStartTime(), docBson.getDate("startTime"));
        assertEquals(stepExecutionDocument.getEndTime(), docBson.getDate("endTime"));
        assertEquals(stepExecutionDocument.getLastUpdated(), docBson.getDate("lastUpdated"));
        assertEquals(stepExecutionDocument.getExitCode(), docBson.getString("exitCode"));
        assertEquals(stepExecutionDocument.getExitDescription(), docBson.getString("exitDescription"));
        assertEquals(stepExecutionDocument.getExecutionContext(), docBson.getString("executionContext"));
        assertEquals(17, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(StepExecutionDocument.class, docBson);

        assertEquals(stepExecutionDocument, resultDoc);
    }

    @Test
    void mongoTemplateConverter_nulls() {
        stepExecutionDocument = new StepExecutionDocument();
        var docBson = (org.bson.Document) mongoTemplate.getConverter().convertToMongoType(stepExecutionDocument);

        assertEquals(0, docBson.size());

        var resultDoc = mongoTemplate.getConverter().read(StepExecutionDocument.class, docBson);

        assertEquals(stepExecutionDocument, resultDoc);
    }
}