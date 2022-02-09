package wirednerd.springbatch.mongo.configuration;

import lombok.NoArgsConstructor;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.lang.Nullable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.util.Assert;
import wirednerd.springbatch.mongo.explore.MongodbJobExplorer;
import wirednerd.springbatch.mongo.repository.MongodbJobRepository;

import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

/**
 * <p>Primary class for enabling Mongodb for storing Spring Batch job execution data.</p>
 * <p>{@link MongoTemplate} and {@link MongoTransactionManager} instances are required.
 * And it is stronly recommended to {@link EnableTransactionManagement}</p>
 * <p>Example Configuration:</p>
 * <pre>
 * &#64;Configuration
 * &#64;EnableTransactionManagement
 * public class BatchConfiguration {
 *
 *     &#64;Bean
 *     MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
 *         return new MongoTransactionManager(mongoDatabaseFactory);
 *     }
 *
 *     &#64;Bean
 *     BatchConfigurer batchConfigurer(MongoTransactionManager mongoTransactionManager, MongoTemplate mongoTemplate) {
 *         return MongodbBatchConfigurer.builder()
 *                 .mongoTemplate(mongoTemplate)
 *                 .mongoTransactionManager(mongoTransactionManager)
 *                 .build();
 *     }
 * }
 * </pre>
 *
 * <p>In the jobCollection, creates 4 Indexes:</p>
 * <ul>
 * <li>Unique Index on jobName, jobKey, and jobExecutionId named "jobInstance_jobExecution_unique"</li>
 * <li>Unique Index on jobExecutionId named "jobExecutionId_unique"</li>
 * <li>Index on jobInstanceId named "jobInstanceId"</li>
 * <li>Index on jobName, jobInstanceId named "jobName_jobInstanceId"</li>
 * </ul>
 *
 * @author Peter Busch
 */
public class MongodbBatchConfigurer implements BatchConfigurer {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final SimpleJobLauncher jobLauncher;
    private final JobExplorer jobExplorer;

    /**
     * MongodbBatchConfigurer can be created using this constructor or the static {@link MongodbBatchConfigurer.Builder}
     *
     * @param mongoTemplate         to be used for Spring Batch job execution data.
     * @param jobCollectionName     to be used for storing job execution data.
     * @param counterCollectionName to be used for storing sequence objects.
     * @param transactionManager    that can be used to manage transactions on the provided MongoTemplate
     * @param taskExecutor          will be used when building the {@link SimpleJobLauncher}
     */
    public MongodbBatchConfigurer(final MongoTemplate mongoTemplate,
                                  final String jobCollectionName, final String counterCollectionName,
                                  final MongoTransactionManager transactionManager,
                                  @Nullable final TaskExecutor taskExecutor) {

        Assert.notNull(mongoTemplate, "A MongoTemplate is required");
        Assert.notNull(transactionManager, "A MongoTransactionManager is required");
        Assert.hasLength(jobCollectionName, "Job Collection Name must not be null or blank");
        Assert.hasLength(counterCollectionName, "Counter Collection Name must not be null or blank");

        this.jobRepository = new MongodbJobRepository(mongoTemplate, jobCollectionName, counterCollectionName);
        this.transactionManager = transactionManager;

        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        try {
            jobLauncher.afterPropertiesSet();
        } catch (Exception e) { //NOPMD
            throw new RuntimeException(e.getMessage(), e); //NOPMD
        }

        jobExplorer = new MongodbJobExplorer(mongoTemplate, jobCollectionName);

        mongoTemplate.indexOps(jobCollectionName)
                .ensureIndex(new Index()
                        .on(JOB_NAME, Sort.Direction.ASC)
                        .on(JOB_KEY, Sort.Direction.ASC)
                        .on(JOB_EXECUTION_ID, Sort.Direction.DESC)
                        .named("jobInstance_jobExecution_unique")
                        .unique());

        mongoTemplate.indexOps(jobCollectionName)
                .ensureIndex(new Index()
                        .on(JOB_EXECUTION_ID, Sort.Direction.DESC)
                        .named("jobExecutionId_unique")
                        .unique());

        mongoTemplate.indexOps(jobCollectionName)
                .ensureIndex(new Index()
                        .on(JOB_INSTANCE_ID, Sort.Direction.DESC)
                        .named("jobInstanceId"));

        mongoTemplate.indexOps(jobCollectionName)
                .ensureIndex(new Index()
                        .on(JOB_NAME, Sort.Direction.ASC)
                        .on(JOB_INSTANCE_ID, Sort.Direction.DESC)
                        .named("jobName_jobInstanceId"));
    }

    @Override
    public JobRepository getJobRepository() {
        return jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() {
        return jobExplorer;
    }

    /**
     * @return Builder for {@link MongodbBatchConfigurer}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link MongodbBatchConfigurer}
     * <p>Example Usage:</p>
     * <pre>
     * MongodbBatchConfigurer.builder()
     *    .mongoTemplate(mongoTemplate)
     *    .mongoTransactionManager(mongoTransactionManager)
     *    .build();
     * </pre>
     *
     * @author Peter Busch
     */
    @SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
    @NoArgsConstructor
    public static class Builder {

        private MongoTemplate mongoTemplate;
        private String jobCollectionName = DEFAULT_JOB_COLLECTION;
        private String counterCollectionName = DEFAULT_COUNTER_COLLECTION;
        private MongoTransactionManager mongoTransactionManager;
        private TaskExecutor taskExecutor;

        /**
         * Specify a {@link MongoTemplate} to be used for Spring Batch job execution data.
         *
         * @param mongoTemplate to be used for Spring Batch job execution data.
         * @return {@link Builder}
         */
        public Builder mongoTemplate(final MongoTemplate mongoTemplate) {
            this.mongoTemplate = mongoTemplate;
            return this;
        }

        /**
         * Use to specify the collection name to use for Job Execution Data.  Defaults to "jobExecutions"
         *
         * @param jobCollectionName collection name to use for Job Execution Data
         * @return {@link Builder}
         */
        public Builder jobCollectionName(final String jobCollectionName) {
            this.jobCollectionName = jobCollectionName;
            return this;
        }

        /**
         * Use to specify the collection name to use for Sequence Objects.  Defaults to "counters"
         *
         * @param counterCollectionName collection name to use for Sequence Objects
         * @return {@link Builder}
         */
        public Builder counterCollectionName(final String counterCollectionName) {
            this.counterCollectionName = counterCollectionName;
            return this;
        }

        /**
         * Specify a {@link MongoTransactionManager} that can be used to manage transactions on the provided {@link MongoTemplate}
         *
         * @param mongoTransactionManager that can be used to manage transactions on the provided {@link MongoTemplate}
         * @return {@link Builder}
         */
        public Builder mongoTransactionManager(final MongoTransactionManager mongoTransactionManager) {
            this.mongoTransactionManager = mongoTransactionManager;
            return this;
        }

        /**
         * {@link TaskExecutor} that will be used when building the {@link JobLauncher} (not required)
         *
         * @param taskExecutor that will be used when building the {@link JobLauncher}
         * @return {@link Builder}
         */
        public Builder taskExecutor(final TaskExecutor taskExecutor) {
            this.taskExecutor = taskExecutor;
            return this;
        }

        /**
         * Build a {@link MongodbBatchConfigurer} using the provided values.
         *
         * @return {@link MongodbBatchConfigurer}
         */
        public MongodbBatchConfigurer build() {
            return new MongodbBatchConfigurer(mongoTemplate, jobCollectionName, counterCollectionName,
                    mongoTransactionManager, taskExecutor);
        }
    }
}
