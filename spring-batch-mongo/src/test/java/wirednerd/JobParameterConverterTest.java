package wirednerd;


import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.OffsetDateTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EnableAutoConfiguration
@MongoUnitTest
class JobParameterConverterTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    private JobParameterWriteConverter jobParameterWriteConverter = new JobParameterWriteConverter();
    private JobParameterReadConverter jobParameterReadConverter = new JobParameterReadConverter();

    @Test
    void mongoInsertAndFind_String() {
        mongoInsertAndFind(new JobParameter("Test Value"));
    }

    @Test
    void mongoInsertAndFind_Date() {
        mongoInsertAndFind(new JobParameter(Date.from(OffsetDateTime.now().toInstant()), false));
    }

    @Test
    void mongoInsertAndFind_Long() {
        mongoInsertAndFind(new JobParameter(123L, true));
    }

    @Test
    void mongoInsertAndFind_Double() {
        mongoInsertAndFind(new JobParameter(1.2, false));
    }

    private void mongoInsertAndFind(JobParameter expected) {
        mongoTemplate.insert(expected);

        var actual = mongoTemplate.findOne(new Query(), JobParameter.class);

        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getValue(), actual.getValue());
        assertEquals(expected.isIdentifying(), actual.isIdentifying());
    }

    @Test
    void convert_JobParameterToDocument_String() {
        var jobParameter = new JobParameter("Test Value");

        var doc = jobParameterWriteConverter.convert(jobParameter);

        assertEquals(1, doc.keySet().size());
        assertTrue(doc.containsKey("STRING"), doc.keySet().toString());
        assertEquals("Test Value", doc.getString("STRING"));
    }

    @Test
    void convert_JobParameterToDocument_NotIdentifying() {
        var jobParameter = new JobParameter("Test Value", false);

        var doc = jobParameterWriteConverter.convert(jobParameter);

        assertEquals(2, doc.keySet().size(), doc.keySet().toString());
        assertTrue(doc.containsKey("identifying"), doc.keySet().toString());
        assertEquals(false, doc.getBoolean("identifying"));
        assertTrue(doc.containsKey("STRING"), doc.keySet().toString());
        assertEquals("Test Value", doc.getString("STRING"));
    }

    @Test
    void convert_JobParameterToDocument_Date() {
        var jobParameter = new JobParameter(Date.from(OffsetDateTime.now().toInstant()));

        var doc = jobParameterWriteConverter.convert(jobParameter);

        assertEquals(1, doc.keySet().size());
        assertTrue(doc.containsKey("DATE"), doc.keySet().toString());
        assertEquals(jobParameter.getValue(), doc.getDate("DATE"));
    }

    @Test
    void convert_JobParameterToDocument_Long() {
        var jobParameter = new JobParameter(123L);

        var doc = jobParameterWriteConverter.convert(jobParameter);

        assertEquals(1, doc.keySet().size());
        assertTrue(doc.containsKey("LONG"), doc.keySet().toString());
        assertEquals(123L, doc.getLong("LONG"));
    }

    @Test
    void convert_JobParameterToDocument_Double() {
        var jobParameter = new JobParameter(1.23);

        var doc = jobParameterWriteConverter.convert(jobParameter);

        assertEquals(1, doc.keySet().size());
        assertTrue(doc.containsKey("DOUBLE"), doc.keySet().toString());
        assertEquals(1.23, doc.getDouble("DOUBLE"));
    }

    @Test
    void convert_DocumentToJobParameter_String() {
        var doc = new Document();
        doc.put("STRING", "Text Value");

        var jobParameter = jobParameterReadConverter.convert(doc);

        assertEquals(JobParameter.ParameterType.STRING, jobParameter.getType());
        assertEquals("Text Value", jobParameter.getValue());
        assertTrue(jobParameter.isIdentifying());
    }
}