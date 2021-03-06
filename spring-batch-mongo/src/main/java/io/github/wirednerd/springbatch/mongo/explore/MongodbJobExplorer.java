package io.github.wirednerd.springbatch.mongo.explore;

import io.github.wirednerd.springbatch.document.JobExecutionDocument;
import io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper;
import io.github.wirednerd.springbatch.document.JobInstanceDocument;
import lombok.Getter;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;
import static io.github.wirednerd.springbatch.mongo.MongodbRepositoryConstants.ID;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

/**
 * <p>Implementation of a {@link JobExplorer} that uses MongoDB instead of a jdbc database.</p>
 * <p>It uses one one collection for storing all job execution data</p>
 *
 * @author Peter Busch
 */
@SuppressWarnings("SameNameButDifferent")
public class MongodbJobExplorer implements JobExplorer {

    /**
     * {@link MongoTemplate} used to access this JobRepository.
     *
     * @return {@link MongoTemplate} used to access this JobRepository.
     */
    @Getter
    private final MongoTemplate mongoTemplate;

    /**
     * Collection where Job Execution Data is being stored.
     *
     * @return Collection where Job Execution Data is being stored.
     */
    @Getter
    private final String jobCollectionName;

    /**
     * Used for converting Batch Job Execution Objects to Document objects
     *
     * @return {@link JobExecutionDocumentMapper}
     */
    @Getter
    private final JobExecutionDocumentMapper jobExecutionDocumentMapper;

    /**
     * @param mongoTemplate              {@link MongoTemplate} to use.
     * @param jobCollectionName          where the job execution data is stored.
     * @param jobExecutionDocumentMapper used for converting Job Execution data
     */
    public MongodbJobExplorer(MongoTemplate mongoTemplate, String jobCollectionName, JobExecutionDocumentMapper jobExecutionDocumentMapper) {
        this.mongoTemplate = mongoTemplate;
        this.jobCollectionName = jobCollectionName;
        this.jobExecutionDocumentMapper = jobExecutionDocumentMapper;
    }

    /**
     * @param instanceId {@link Long} id for the jobInstance to obtain.
     * @return the {@link JobInstance} with this id, or null
     */
    @Override
    public JobInstance getJobInstance(Long instanceId) {
        var document = mongoTemplate.findOne(Query
                        .query(Criteria.where(JOB_INSTANCE_ID).is(instanceId)),
                JobInstanceDocument.class, jobCollectionName);

        return document == null ? null : jobExecutionDocumentMapper.toJobInstance(document);
    }

    /**
     * Query the repository for all unique {@link JobInstance} names (sorted
     * alphabetically).
     *
     * @return the set of job names that have been executed
     */
    @Override
    public List<String> getJobNames() {

        var query = newAggregation(
                group(JOB_NAME),
                sort(Sort.by(ID).ascending()));

        return mongoTemplate.aggregate(query, jobCollectionName, Document.class)
                .getMappedResults()
                .stream().map(doc -> doc.getString(ID))
                .collect(Collectors.toList());
    }

    /**
     * Query the repository for the number of unique {@link JobInstance}s
     * associated with the supplied job name.
     *
     * @param jobName the name of the job to query for
     * @return the number of {@link JobInstance}s that exist within the
     * associated job repository
     * @throws NoSuchJobException thrown when there is no {@link JobInstance}
     *                            for the jobName specified.
     */
    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException {

        var query = newAggregation(
                match(Criteria.where(JOB_NAME).is(jobName)),
                group(JOB_INSTANCE_ID));

        int count = mongoTemplate
                .aggregate(query, jobCollectionName, Document.class)
                .getMappedResults().size();

        if (count == 0) {
            throw new NoSuchJobException("No job instances were found for job name " + jobName);
        }

        return count;
    }

    /**
     * Fetch {@link JobInstance} values in descending order of creation (and
     * therefore usually of first execution).
     *
     * @param jobName the name of the job to query
     * @param start   the start index of the instances to return
     * @param count   the maximum number of instances to return
     * @return the {@link JobInstance} values up to a maximum of count values
     */
    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {

        var query = newAggregation(
                match(Criteria.where(JOB_NAME).is(jobName)),
                group(JOB_NAME, JOB_INSTANCE_ID),
                replaceRoot(ID),
                sort(Sort.by(JOB_INSTANCE_ID).descending()),
                skip((long) start),
                limit(count));

        return mongoTemplate
                .aggregate(query, jobCollectionName, JobInstanceDocument.class)
                .getMappedResults()
                .stream().map(jobExecutionDocumentMapper::toJobInstance)
                .collect(Collectors.toList());
    }

    /**
     * Fetch {@link JobInstance} values in descending order of creation (and
     * there for usually of first execution) with a 'like'/wildcard criteria.
     *
     * @param jobName the name of the job to query for.
     * @param start   the start index of the instances to return.
     * @param count   the maximum number of instances to return.
     * @return a list of {@link JobInstance} for the job name requested.
     */
    @Override
    public List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {

        String jobNameRegex = jobName.replaceAll("\\*|%", ".*");

        var query = newAggregation(
                match(Criteria.where(JOB_NAME).regex(jobNameRegex)),
                group(JOB_NAME, JOB_INSTANCE_ID),
                replaceRoot(ID),
                sort(Sort.by(JOB_INSTANCE_ID).descending()),
                skip((long) start),
                limit(count));

        return mongoTemplate
                .aggregate(query, jobCollectionName, JobInstanceDocument.class)
                .getMappedResults()
                .stream().map(jobExecutionDocumentMapper::toJobInstance)
                .collect(Collectors.toList());
    }

    /**
     * Find the last job instance by id for the given job.
     *
     * @param jobName name of the job
     * @return the last job instance by id if any or null otherwise
     */
    @Override
    public JobInstance getLastJobInstance(String jobName) {
        var document = mongoTemplate.findOne(Query.query(Criteria.where(JOB_NAME).is(jobName))
                        .with(Sort.by(JOB_INSTANCE_ID).descending())
                        .limit(1),
                JobInstanceDocument.class, jobCollectionName);

        return document == null ? null : jobExecutionDocumentMapper.toJobInstance(document);
    }

    /**
     * Retrieve a {@link JobExecution} by its id.
     * The returned data will be fully hydrated.
     *
     * @param executionId the job execution id
     * @return the {@link JobExecution} with this id, or null if not found
     */
    @Override
    public JobExecution getJobExecution(Long executionId) {
        var document = mongoTemplate.findOne(Query
                        .query(Criteria.where(JOB_EXECUTION_ID).is(executionId)),
                JobExecutionDocument.class, jobCollectionName);

        return document == null ? null : jobExecutionDocumentMapper.toJobExecution(document);
    }

    /**
     * Retrieve job executions by their job instance.
     * The returned data will be fully hydrated.
     *
     * @param jobInstance the {@link JobInstance} to query
     * @return the set of all executions for the specified {@link JobInstance}
     */
    @Override
    public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "JobInstance must not be null.");

        return mongoTemplate.find(Query
                                .query(Criteria.where(JOB_INSTANCE_ID).is(jobInstance.getId()))
                                .with(Sort.by(JOB_EXECUTION_ID).descending()),
                        JobExecutionDocument.class, jobCollectionName)
                .stream().map(jobExecutionDocumentMapper::toJobExecution)
                .collect(Collectors.toList());
    }

    /**
     * Find the last {@link JobExecution} that has been created for a given
     * {@link JobInstance}.
     *
     * @param jobInstance the {@link JobInstance}
     * @return the last {@link JobExecution} that has been created for this instance or
     * {@code null} if no job execution is found for the given job instance.
     */
    @Override
    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "JobInstance must not be null.");

        var document = mongoTemplate.findOne(Query
                        .query(Criteria.where(JOB_INSTANCE_ID).is(jobInstance.getId()))
                        .with(Sort.by(JOB_EXECUTION_ID).descending())
                        .limit(1),
                JobExecutionDocument.class, jobCollectionName);

        return document == null ? null : jobExecutionDocumentMapper.toJobExecution(document);
    }

    /**
     * Find the last {@link JobExecution} for specified jobName that completed successfully
     *
     * @param jobName name of the job
     * @return the last {@link JobExecution} with the specified jobName and "Completed" status.
     * @since 1.1.3
     */
    public JobExecution getLastCompletedJobExecution(String jobName) {
        var executionDoc = mongoTemplate.findOne(Query
                        .query(Criteria.where(JOB_NAME).is(jobName)
                                .and(STATUS).is(BatchStatus.COMPLETED)
                                .and(EXIT_CODE).is("COMPLETED"))
                        .with(Sort.by(JOB_EXECUTION_ID).descending()),
                JobExecutionDocument.class, jobCollectionName);

        if (executionDoc != null) {
            return this.getJobExecutionDocumentMapper().toJobExecution(executionDoc);
        }

        return null;
    }

    /**
     * Retrieve running job executions.
     * The returned data will be fully hydrated.
     *
     * @param jobName the name of the job
     * @return the set of running executions for jobs with the specified name
     */
    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        return mongoTemplate.find(Query.query(Criteria.where(JOB_NAME).is(jobName))
                                .addCriteria(Criteria.where(START_TIME).ne(null))
                                .addCriteria(Criteria.where(END_TIME).is(null))
                                .with(Sort.by(JOB_EXECUTION_ID).descending()),
                        JobExecutionDocument.class, jobCollectionName)
                .stream().map(jobExecutionDocumentMapper::toJobExecution)
                .collect(Collectors.toSet());
    }

    /**
     * Retrieve a {@link StepExecution} by its id and parent
     * {@link JobExecution} id.
     * The returned data will be fully hydrated.
     *
     * @param jobExecutionId  the parent job execution id
     * @param stepExecutionId the step execution id
     * @return the {@link StepExecution} with this id, or null if not found
     */
    @Override
    public StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {

        Assert.notNull(jobExecutionId, "jobExecutionId must not be null.");
        Assert.notNull(stepExecutionId, "stepExecutionId must not be null.");

        var document = mongoTemplate.findOne(Query
                        .query(Criteria.where(JOB_EXECUTION_ID).is(jobExecutionId)),
                JobExecutionDocument.class, jobCollectionName);

        if (document == null) {
            return null;
        }

        var jobExecution = jobExecutionDocumentMapper.toJobExecution(document);

        if (CollectionUtils.isEmpty(jobExecution.getStepExecutions())) {
            return null;
        }

        for (var step : jobExecution.getStepExecutions()) {
            if (stepExecutionId.equals(step.getId())) {
                return step;
            }
        }

        return null;
    }
}
