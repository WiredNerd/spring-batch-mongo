package io.github.wirednerd.springbatch.document;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.mongodb.core.MongoTemplate;

@DataMongoTest
public class MongoDBContainerConfig {

    @Autowired
    protected MongoTemplate mongoTemplate;

    @BeforeEach
    void resetCollections() {
        mongoTemplate.getCollectionNames().forEach(mongoTemplate::dropCollection);
    }
}
