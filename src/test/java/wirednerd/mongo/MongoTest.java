package wirednerd.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@MongoUnitTest
public class MongoTest {

    @Autowired
    MongoTemplate mongoTemplate;

    @Test
    public void test() {
        // given
        DBObject objectToSave = BasicDBObjectBuilder.start()
                .add("key", "value")
                .get();

        // when
        mongoTemplate.save(objectToSave, "collection");

        // then
        assertThat(mongoTemplate.findAll(DBObject.class, "collection")).extracting("key")
                .containsOnly("value");
    }
}
