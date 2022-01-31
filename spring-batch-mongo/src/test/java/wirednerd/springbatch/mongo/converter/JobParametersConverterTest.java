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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@MongoUnitTest
public class JobParametersConverterTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void mongoInsertAndFind_String() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L));

        var expected = new JobParameters(paramMap);

        mongoTemplate.insert(JobParametersConverter.convert(expected), "Test");
        var actual = JobParametersConverter.convert(mongoTemplate.findOne(new Query(), Document.class, "Test"));

        compare(expected, actual);
    }

    public static void compare(JobParameters expected, JobParameters actual) {
        assertEquals(expected.getParameters().size(), actual.getParameters().size());
        for (var key : expected.getParameters().keySet()) {
            JobParameterConverterTest.compare(expected.getParameters().get(key), actual.getParameters().get(key));
        }
    }

    @Test
    void convert_JobParametersToDocument() {
        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Test String Key", new JobParameter("Test Value"));
        paramMap.put("Test Long Key", new JobParameter(123L));

        var jobParameters = new JobParameters(paramMap);

        var doc = JobParametersConverter.convert(jobParameters);

        assertEquals(2, doc.keySet().size());
        assertTrue(doc.containsKey("Test String Key"));
        assertEquals("Test Value", doc.get("Test String Key", Document.class).getString("STRING"));
        assertTrue(doc.containsKey("Test Long Key"));
        assertEquals(123L, doc.get("Test Long Key", Document.class).getLong("LONG"));
    }

    @Test
    void convert_DocumentToJobParameters() {
        var doc = new Document();
        doc.put("Test String Key", JobParameterConverter.convert(new JobParameter("Test Value")));
        doc.put("Test Long Key", JobParameterConverter.convert(new JobParameter(123L)));

        var jobParameters = JobParametersConverter.convert(doc);

        assertEquals(2, jobParameters.getParameters().size());
        assertEquals("Test Value", jobParameters.getString("Test String Key"));
        assertEquals(123L, jobParameters.getLong("Test Long Key"));
    }
}
