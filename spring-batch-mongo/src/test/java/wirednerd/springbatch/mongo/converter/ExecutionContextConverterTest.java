package wirednerd.springbatch.mongo.converter;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mongounit.MongoUnitTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@MongoUnitTest
public
class ExecutionContextConverterTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void mongoInsertAndFind() {
        var expected = new ExecutionContext();

        expected.putString("Value 1", "String Value");
        expected.putLong("Value 2", 123L);
        expected.putDouble("Value 3", 1.23);
        expected.putInt("Value 4", 123);

        var innerDoc = new Document();
        innerDoc.put("Inner Value 1", "String Value 2");
        expected.put("Value 5", innerDoc);

        mongoTemplate.insert(ExecutionContextConverter.convert(expected), "Test");
        var actual = ExecutionContextConverter.convert(mongoTemplate.findOne(new Query(), Document.class, "Test"));

        compare(expected, actual);
    }

    public static void compare(ExecutionContext expected, ExecutionContext actual) {
        if (expected == null) {
            assertTrue(actual == null || actual.entrySet().size() == 0);
            return;
        }
        for (var entry : expected.entrySet()) {
            assertTrue(actual.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), actual.get(entry.getKey()));
        }
    }

    @Test
    void convert_ExecutionContextToDocument() {
        var executionContext = new ExecutionContext();

        executionContext.putString("Value 1", "String Value");
        executionContext.putLong("Value 2", 123L);
        executionContext.putDouble("Value 3", 1.23);
        executionContext.putInt("Value 4", 123);

        var innerDoc = new Document();
        innerDoc.put("Inner Value 1", "String Value 2");
        executionContext.put("Value 5", innerDoc);

        var document = ExecutionContextConverter.convert(executionContext);

        assertEquals(5, document.size());

        assertEquals("String Value", document.getString("Value 1"));
        assertEquals(123L, document.getLong("Value 2"));
        assertEquals(1.23, document.getDouble("Value 3"));
        assertEquals(123, document.getInteger("Value 4"));
        assertEquals(innerDoc, document.get("Value 5", Document.class));
    }

    @Test
    void convert_ExecutionContextToDocument_null() {
        var document = ExecutionContextConverter.convert((ExecutionContext) null);
        assertEquals(0, document.size());
    }

    @Test
    void convert_ExecutionContextToDocument_empty() {
        var document = ExecutionContextConverter.convert(new ExecutionContext());
        assertEquals(0, document.size());
    }

    @Test
    void convert_DocumentToExecutionContext() {
        var document = new Document();

        document.put("Value 1", "String Value");
        document.put("Value 2", 123L);
        document.put("Value 3", 1.23);
        document.put("Value 4", 123);

        var innerDoc = new Document();
        innerDoc.put("Inner Value 1", "String Value 2");
        document.put("Value 5", innerDoc);

        var executionContext = ExecutionContextConverter.convert(document);

        assertEquals(5, executionContext.entrySet().size());

        assertEquals("String Value", executionContext.getString("Value 1"));
        assertEquals(123L, executionContext.getLong("Value 2"));
        assertEquals(1.23, executionContext.getDouble("Value 3"));
        assertEquals(123, executionContext.getInt("Value 4"));
        assertEquals(innerDoc, executionContext.get("Value 5"));
    }

    @Test
    void convert_DocumentToExecutionContext_null() {
        var executionContext = ExecutionContextConverter.convert((Document) null);
        assertEquals(0, executionContext.entrySet().size());
    }

    @Test
    void convert_DocumentToExecutionContext_empty() {
        var executionContext = ExecutionContextConverter.convert(new Document());
        assertEquals(0, executionContext.entrySet().size());
    }
}