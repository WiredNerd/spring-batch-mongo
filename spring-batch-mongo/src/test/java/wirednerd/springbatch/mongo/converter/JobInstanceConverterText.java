package wirednerd.springbatch.mongo.converter;

import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@MongoUnitTest
public class JobInstanceConverterText {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void mongoInsertAndFind() {
        var jobInstance = new JobInstance(123L, "Job Name");
        jobInstance.setVersion(567);

        mongoTemplate.insert(jobInstance);

        var actual = mongoTemplate.findOne(new Query(), JobInstance.class);

        assertEquals("Job Name", jobInstance.getJobName());
        assertEquals(123L, jobInstance.getId());
        assertEquals(567, jobInstance.getVersion());
    }

}
