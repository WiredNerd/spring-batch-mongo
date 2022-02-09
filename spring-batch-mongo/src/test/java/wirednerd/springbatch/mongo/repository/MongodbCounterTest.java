package wirednerd.springbatch.mongo.repository;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import wirednerd.springbatch.mongo.MongoDBContainerConfig;

import static org.junit.jupiter.api.Assertions.*;

class MongodbCounterTest extends MongoDBContainerConfig {

    @Test
    void constructor() {
        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        assertNotNull(counter.getMongoTemplate());
        assertEquals("testCounter", counter.getCounterName());
        assertEquals("testCounters", counter.getCounterCollection());

        var counterDoc = mongoTemplate.find(new Query().addCriteria(Criteria
                .where("counter").is("testCounter")), Document.class, "testCounters");

        assertEquals(1, counterDoc.size());
        assertEquals(0L, counterDoc.get(0).getLong("value"));
    }

    @Test
    void constructor_ensureIndex() {
        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        var counterIndexes = mongoTemplate.indexOps("testCounters").getIndexInfo();
        assertEquals(2, counterIndexes.size());
        assertEquals("_id_", counterIndexes.get(0).getName());

        assertEquals("counter_unique", counterIndexes.get(1).getName());
        var counterIndexeFields = counterIndexes.get(1).getIndexFields();
        assertEquals(1, counterIndexeFields.size());
        assertEquals("counter", counterIndexeFields.get(0).getKey());
        assertTrue(counterIndexes.get(1).isUnique());
    }

    @Test
    void constructor_existingCounter() {
        var existingCounterDoc = new Document();
        existingCounterDoc.put("counter", "testCounter");
        existingCounterDoc.put("value", 10L);
        mongoTemplate.insert(existingCounterDoc, "testCounters");

        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        assertNotNull(counter.getMongoTemplate());
        assertEquals("testCounter", counter.getCounterName());
        assertEquals("testCounters", counter.getCounterCollection());

        var counterDoc = mongoTemplate.find(new Query().addCriteria(Criteria
                .where("counter").is("testCounter")), Document.class, "testCounters");

        assertEquals(1, counterDoc.size());
        assertEquals(10L, counterDoc.get(0).getLong("value"));
    }

    @Test
    void nextValue() {
        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        assertEquals(1L, counter.nextValue());
        assertEquals(2L, counter.nextValue());
        assertEquals(3L, counter.nextValue());
    }

    @Test
    void nextValue_existingCounter() {
        var existingCounterDoc = new Document();
        existingCounterDoc.put("counter", "testCounter");
        existingCounterDoc.put("value", 10L);
        mongoTemplate.insert(existingCounterDoc, "testCounters");

        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        assertEquals(11L, counter.nextValue());
        assertEquals(12L, counter.nextValue());
        assertEquals(13L, counter.nextValue());
    }

    @Test
    void nextValue_counterMissing() {
        var counter = new MongodbCounter(mongoTemplate, "testCounter", "testCounters");

        assertEquals(1L, counter.nextValue());

        mongoTemplate.remove(new Query().addCriteria(Criteria.where("counter").is("testCounter")), "testCounters");

        try {
            counter.nextValue();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertEquals("Could not find counter: testCounter", e.getMessage());
        }
    }
}