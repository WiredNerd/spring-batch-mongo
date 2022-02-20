package io.github.wirednerd.springbatch.document;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;

@DataMongoTest
//@DirtiesContext
public class MongoDBContainerConfig {

    @Autowired
    protected MongoTemplate mongoTemplate;

//    @Autowired
//    protected MongoDatabaseFactory mongoDatabaseFactory;

    @BeforeEach
    void resetCollections() {
        mongoTemplate.getCollectionNames().forEach(mongoTemplate::dropCollection);
    }
}
