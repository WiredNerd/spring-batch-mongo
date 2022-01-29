package wirednerd.springbatch.mongo.converter;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@MongoUnitTest
public class JobParametersConverterTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    private JobParametersReadConverter jobParametersReadConverter = new JobParametersReadConverter();
    private JobParametersWriteConverter jobParametersWriteConverter = new JobParametersWriteConverter();

    private JobParameterWriteConverter jobParameterWriteConverter = new JobParameterWriteConverter();

    @Test
    void mongoInsertAndFind() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L, false));

        var jobParameters = new JobParameters(paramMap);

        mongoTemplate.insert(jobParameters);

        var actual = mongoTemplate.findOne(new Query(), JobParameters.class);

        assertEquals(2, actual.getParameters().size());
        assertEquals("Test Value", actual.getString("Test String Key"));
        assertTrue(actual.getParameters().get("Test String Key").isIdentifying());
        assertEquals(123L, actual.getLong("Test Long Key"));
        assertFalse(actual.getParameters().get("Test Long Key").isIdentifying());
    }

    @Test
    void convert_JobParametersToDocument() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L));

        var jobParameters = new JobParameters(paramMap);

        var doc = jobParametersWriteConverter.convert(jobParameters);

        assertEquals(2, doc.keySet().size());
        assertTrue(doc.containsKey("Test String Key"));
        assertEquals("Test Value", doc.get("Test String Key", Document.class).getString("STRING"));
        assertTrue(doc.containsKey("Test Long Key"));
        assertEquals(123L, doc.get("Test Long Key", Document.class).getLong("LONG"));
    }

    @Test
    void convert_DocumentToJobParameters() {
        var doc = new Document();
        doc.put("Test String Key", jobParameterWriteConverter.convert(new JobParameter("Test Value")));
        doc.put("Test Long Key", jobParameterWriteConverter.convert(new JobParameter(123L)));

        var jobParameters = jobParametersReadConverter.convert(doc);

        assertEquals(2, jobParameters.getParameters().size());
        assertEquals("Test Value", jobParameters.getString("Test String Key"));
        assertEquals(123L, jobParameters.getLong("Test Long Key"));
    }
}
