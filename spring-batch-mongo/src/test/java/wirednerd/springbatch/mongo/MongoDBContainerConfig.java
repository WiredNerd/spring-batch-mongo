package wirednerd.springbatch.mongo;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoAutoConfiguration;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@DataMongoTest(excludeAutoConfiguration = EmbeddedMongoAutoConfiguration.class)
@Testcontainers
@DirtiesContext
public class MongoDBContainerConfig {

    @Container
    private static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("mongo", "5.0"));

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    protected MongoDatabaseFactory mongoDatabaseFactory;

    @BeforeEach
    void resetCollections() {
        mongoTemplate.getCollectionNames().forEach(mongoTemplate::dropCollection);
    }
}
