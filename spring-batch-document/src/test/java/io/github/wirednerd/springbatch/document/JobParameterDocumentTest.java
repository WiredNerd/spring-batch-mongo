package io.github.wirednerd.springbatch.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameter;
import org.springframework.data.mongodb.core.query.Query;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class JobParameterDocumentTest extends MongoDBContainerConfig {

    private Date testDate = Date.from(OffsetDateTime.of(2022, 2, 19, 1, 2, 4, 5_000_000, ZoneOffset.UTC).toInstant());

    private JobParameterDocument jobParameterDocument = new JobParameterDocument("Test Value",
            testDate, 123L, 3.14, true);

    public static void compare(JobParameter expected, JobParameter actual) {
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getValue(), actual.getValue());
        assertEquals(expected.isIdentifying(), actual.isIdentifying());
    }

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

    @Test
    void fromJobParameterString() {
        var doc = JobParameterDocument.from(new JobParameter("Test Value", false));

        assertEquals("Test Value", doc.getStringValue());
        assertEquals(null, doc.getDateValue());
        assertEquals(null, doc.getLongValue());
        assertEquals(null, doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = JobParameterDocument.from(new JobParameter("Test Value", true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void fromJobParameterDate() {
        var doc = JobParameterDocument.from(new JobParameter(testDate, false));

        assertEquals(null, doc.getStringValue());
        assertEquals(testDate, doc.getDateValue());
        assertEquals(null, doc.getLongValue());
        assertEquals(null, doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = JobParameterDocument.from(new JobParameter(testDate, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void fromJobParameterLong() {
        var doc = JobParameterDocument.from(new JobParameter(123L, false));

        assertEquals(null, doc.getStringValue());
        assertEquals(null, doc.getDateValue());
        assertEquals(123L, doc.getLongValue());
        assertEquals(null, doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = JobParameterDocument.from(new JobParameter(123L, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void fromJobParameterDouble() {
        var doc = JobParameterDocument.from(new JobParameter(3.14, false));

        assertEquals(null, doc.getStringValue());
        assertEquals(null, doc.getDateValue());
        assertEquals(null, doc.getLongValue());
        assertEquals(3.14, doc.getDoubleValue());
        assertEquals(false, doc.getIdentifying());

        doc = JobParameterDocument.from(new JobParameter(3.14, true));
        assertNull(doc.getIdentifying());
    }

    @Test
    void toJobParameterString() {
        var jobParameter = new JobParameterDocument("Test Value", null, null, null, null).toJobParameter();

        assertEquals(JobParameter.ParameterType.STRING, jobParameter.getType());
        assertEquals("Test Value", jobParameter.getValue());
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument("Test Value", null, null, null, true).toJobParameter();
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument("Test Value", null, null, null, false).toJobParameter();
        assertEquals(false, jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterDate() {
        var jobParameter = new JobParameterDocument(null, testDate, null, null, null).toJobParameter();

        assertEquals(JobParameter.ParameterType.DATE, jobParameter.getType());
        assertEquals(testDate, jobParameter.getValue());
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, testDate, null, null, true).toJobParameter();
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, testDate, null, null, false).toJobParameter();
        assertEquals(false, jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterLong() {
        var jobParameter = new JobParameterDocument(null, null, 123L, null, null).toJobParameter();

        assertEquals(JobParameter.ParameterType.LONG, jobParameter.getType());
        assertEquals(123L, jobParameter.getValue());
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, null, 123L, null, true).toJobParameter();
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, null, 123L, null, false).toJobParameter();
        assertEquals(false, jobParameter.isIdentifying());
    }

    @Test
    void toJobParameterDouble() {
        var jobParameter = new JobParameterDocument(null, null, null, 3.14, null).toJobParameter();

        assertEquals(JobParameter.ParameterType.DOUBLE, jobParameter.getType());
        assertEquals(3.14, jobParameter.getValue());
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, null, null, 3.14, true).toJobParameter();
        assertEquals(true, jobParameter.isIdentifying());

        jobParameter = new JobParameterDocument(null, null, null, 3.14, false).toJobParameter();
        assertEquals(false, jobParameter.isIdentifying());
    }

    @Test
    void mongoInsertAndFind_String() {
        var expected = new JobParameter("Test Value");

        mongoTemplate.insert(JobParameterDocument.from(expected), "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test").toJobParameter();

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_NotIdentifying() {
        var expected = new JobParameter("Test Value", false);

        mongoTemplate.insert(JobParameterDocument.from(expected), "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test").toJobParameter();

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_Date() {
        var expected = new JobParameter(Date.from(OffsetDateTime.now().toInstant()));

        mongoTemplate.insert(JobParameterDocument.from(expected), "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test").toJobParameter();

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_Long() {
        var expected = new JobParameter(456L);

        mongoTemplate.insert(JobParameterDocument.from(expected), "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test").toJobParameter();

        compare(expected, actual);
    }

    @Test
    void mongoInsertAndFind_Double() {
        var expected = new JobParameter(12.3);

        mongoTemplate.insert(JobParameterDocument.from(expected), "Test");
        var actual = mongoTemplate.findOne(new Query(), JobParameterDocument.class, "Test").toJobParameter();

        compare(expected, actual);
    }
}