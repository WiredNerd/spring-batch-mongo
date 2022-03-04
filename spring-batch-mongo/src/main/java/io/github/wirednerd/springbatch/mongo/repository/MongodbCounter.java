package io.github.wirednerd.springbatch.mongo.repository;

import lombok.Getter;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;

/**
 * <p>This class represents a sequence object stores in a MongoDB collection.</p>
 * <p>Counter documents consist of 2 fields "counter" and "value".
 * "counter" is a String value that will contain the name of the counter.
 * "value" is a Long field that contains the last returned value.</p>
 * <p>It will ensure there is an index on the collection called "counter_unique".
 * This index enforces that the "counter" field is unique.</p>
 * <pre>
 * {
 *    "counter": "&lt;string&gt;",
 *    "value": &lt;long&gt;
 * }</pre>
 *
 * @author Peter Busch
 */
@SuppressWarnings("SameNameButDifferent")
public class MongodbCounter {

    /**
     * {@link MongoTemplate} used to access this counter.
     *
     * @return {@link MongoTemplate} used to access this counter.
     */
    @Getter
    private final MongoTemplate mongoTemplate;

    /**
     * Collection this counter is stored in.
     *
     * @return Collection this counter is stored in.
     */
    @Getter
    private final String counterCollection;

    /**
     * Name for this counter.
     *
     * @return Name for this counter.
     */
    @Getter
    private final String counterName;

    /**
     * Reusable {@link Query} for accessing the counter.
     */
    private transient final Query findCounter;

    private static final String COUNTER_FIELD_NAME = "counter";
    private static final String COUNTER_VALUE_NAME = "value";
    private static final String COUNTER_INDEX_NAME = "counter_unique";

    private static final Update INCREMENT_COUNTER = new Update().inc(COUNTER_VALUE_NAME, 1);
    private static final FindAndModifyOptions RETURN_NEW = new FindAndModifyOptions().returnNew(true);

    /**
     * <p>Create new counter object in the specified Collection using the provided {@link MongoTemplate}</p>
     * <p>If the counter does not exist yet, it will be initialized with value=0</p>
     * <p>This will also ensure there is an index on the collection called "counter_unique".
     * This index enforces that the "counter" field is unique.</p>
     *
     * @param mongoTemplate     {@link MongoTemplate} to use
     * @param counterName       value to use in the "counter" field of the document
     * @param counterCollection collection to use for storing the document.
     */
    public MongodbCounter(final MongoTemplate mongoTemplate, final String counterName, final String counterCollection) {
        this.mongoTemplate = mongoTemplate;
        this.counterCollection = counterCollection;
        this.counterName = counterName;

        findCounter = new Query().addCriteria(Criteria.where(COUNTER_FIELD_NAME).is(counterName));

        mongoTemplate.indexOps(counterCollection)
                .ensureIndex(new Index()
                        .on(COUNTER_FIELD_NAME, Sort.Direction.ASC)
                        .named(COUNTER_INDEX_NAME)
                        .unique());

        mongoTemplate.upsert(findCounter, new Update()
                        .setOnInsert(COUNTER_FIELD_NAME, counterName)
                        .setOnInsert(COUNTER_VALUE_NAME, 0L),
                counterCollection);
    }

    /**
     * Increment the value for this counter in the database, and return the updated value.
     *
     * @return Updated counter value.
     */
    public Long nextValue() {
        var counterDoc = mongoTemplate.findAndModify(findCounter, INCREMENT_COUNTER, RETURN_NEW,
                Document.class, counterCollection);

        Assert.state(counterDoc != null, () -> "Could not find counter: " + counterName);

        return counterDoc.getLong(COUNTER_VALUE_NAME);
    }
}
