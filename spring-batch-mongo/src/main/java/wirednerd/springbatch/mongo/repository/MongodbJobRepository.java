package wirednerd.springbatch.mongo.repository;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import wirednerd.springbatch.mongo.converter.ExecutionContextConverter;
import wirednerd.springbatch.mongo.converter.JobExecutionConverter;
import wirednerd.springbatch.mongo.converter.JobInstanceConverter;
import wirednerd.springbatch.mongo.converter.StepExecutionConverter;

import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

/**
 * <p>Implementation of a {@link JobRepository} that uses MongoDB instead of a jdbc database.</p>
 * <p>It uses one one collection for storing all job execution data, and another for storing counters.  See: {@link MongodbCounter}</p>
 * <p>In the counterCollection, creates Counter objects for jobInstanceId, jobExecutionId, and stepExecutionId</p>
 * <p>Schema for job execution data</p>
 * <pre>
 * {
 *   "jobInstanceId": "&lt;long&gt;",
 *   "jobName": "&lt;string&gt;",
 *   "jobKey": "&lt;string&gt;",
 *   "jobParameters": {
 *     "&lt;stringParameterKey&gt;": {
 *       "STRING": "&lt;string&gt;",
 *       "identifying": "&lt;boolean, default true&gt;"
 *     },
 *     "&lt;dateParameterKey&gt;": {
 *       "DATE": "&lt;date&gt;",
 *       "identifying": "&lt;boolean, default true&gt;"
 *     },
 *     "&lt;longParameterKey&gt;": {
 *       "LONG": "&lt;long&gt;",
 *       "identifying": "&lt;boolean, default true&gt;"
 *     },
 *     "&lt;doubleParameterKey&gt;": {
 *       "DOUBLE": "&lt;double&gt;",
 *       "identifying": "&lt;boolean, default true&gt;"
 *     }
 *   },
 *   "jobExecutionId": "&lt;long&gt;",
 *   "version": "&lt;integer&gt;",
 *   "status": "&lt;string&gt;",
 *   "startTime": "&lt;date&gt;",
 *   "createTime": "&lt;date&gt;",
 *   "endTime": "&lt;date&gt;",
 *   "lastUpdated": "&lt;date&gt;",
 *   "exitCode": "&lt;string&gt;",
 *   "exitDescription": "&lt;string&gt;",
 *   "jobConfigurationName": "&lt;string&gt;",
 *   "executionContext": {
 *     "&lt;key&gt;": "&lt;value&gt;"
 *   },
 *   "stepExecutions": [
 *     {
 *       "stepExecutionId": "&lt;long&gt;",
 *       "stepName": "&lt;string&gt;",
 *       "status": "&lt;string&gt;",
 *       "readCount": "&lt;integer&gt;",
 *       "writeCount": "&lt;integer&gt;",
 *       "commitCount": "&lt;integer&gt;",
 *       "rollbackCount": "&lt;integer&gt;",
 *       "readSkipCount": "&lt;integer&gt;",
 *       "processSkipCount": "&lt;integer&gt;",
 *       "writeSkipCount": "&lt;integer&gt;",
 *       "startTime": "&lt;date&gt;",
 *       "endTime": "&lt;date&gt;",
 *       "lastUpdated": "&lt;date&gt;",
 *       "exitCode": "&lt;string&gt;",
 *       "exitDescription": "&lt;string&gt;",
 *       "filterCount": "&lt;integer&gt;",
 *       "executionContext": {
 *         "&lt;key&gt;": "&lt;value&gt;"
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * @author Peter Busch
 */
@Slf4j
@SuppressWarnings({"PMD.TooManyMethods", "PMD.CommentSize"})
public class MongodbJobRepository implements JobRepository {

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
     * Collection where Counter Data is being stored.
     *
     * @return Collection where Counter Data is being stored.
     */
    @Getter
    private final String counterCollectionName;

    /**
     * Counter for jobInstanceId
     *
     * @return Counter for jobInstanceId
     */
    @Getter
    private final MongodbCounter jobInstanceCounter;

    /**
     * Counter for jobExecutionId
     *
     * @return Counter for jobExecutionId
     */
    @Getter
    private final MongodbCounter jobExecutionCounter;

    /**
     * Counter for stepExecutionId
     *
     * @return Counter for stepExecutionId
     */
    @Getter
    private final MongodbCounter stepExecutionCounter;

    private static JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    /**
     * <p>Initializes Counter objects for jobInstanceId, jobExecutionId, and stepExecutionId</p>
     * <p>Ensures Unique Index on jobName, jobKey, and jobExecutionId named "jobInstance_jobExecution_unique"</p>
     * <p>Ensures Unique Index on jobExecutionId named "jobExecutionId_unique"</p>
     *
     * @param mongoTemplate         {@link MongoTemplate} to use.
     * @param jobCollectionName     to be used for storing job execution data.
     * @param counterCollectionName to be used for storing sequence objects.
     */
    public MongodbJobRepository(MongoTemplate mongoTemplate, String jobCollectionName, String counterCollectionName) {
        this.mongoTemplate = mongoTemplate;
        this.jobCollectionName = jobCollectionName;
        this.counterCollectionName = counterCollectionName;

        jobInstanceCounter = new MongodbCounter(mongoTemplate, JOB_INSTANCE_ID, counterCollectionName);
        jobExecutionCounter = new MongodbCounter(mongoTemplate, JOB_EXECUTION_ID, counterCollectionName);
        stepExecutionCounter = new MongodbCounter(mongoTemplate, STEP_EXECUTION_ID, counterCollectionName);
    }

    /**
     * Check if a JobExecution already exists in the database
     * for this combination of jobName and jobParameters
     *
     * @param jobName       the name of the job
     * @param jobParameters the parameters to match
     * @return true if a {@link JobInstance} already exists for this job name and job parameters
     * @throws IllegalArgumentException if jobName or jobParameters is null/blank
     */
    @Override
    public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
        validateJobInstance(jobName, jobParameters);
        return mongoTemplate.exists(new Query()
                        .addCriteria(Criteria.where(JOB_NAME).is(jobName))
                        .addCriteria(Criteria.where(JOB_KEY).is(jobKeyGenerator.generateKey(jobParameters)))
                        .limit(1),
                jobCollectionName);
    }

    /**
     * Create a new {@link JobInstance} with the name and job parameters provided.
     * Will assign a new, unique identifier to this JobInstance.
     * Stores in database as jobExecution with only jobInstanceId, jobName, and jobKey.
     *
     * @param jobName       logical name of the job
     * @param jobParameters parameters used to execute the job
     * @return the new {@link JobInstance}
     * @throws IllegalArgumentException if jobName or jobParameters is null/blank
     * @throws IllegalStateException    if jobExecution already is created for this combination of JobName and JobParameters
     */
    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {

        Assert.state(!isJobInstanceExists(jobName, jobParameters), "JobInstance must not already exist.");

        var jobInstance = new JobInstance(jobInstanceCounter.nextValue(), jobName);

        mongoTemplate.insert(JobInstanceConverter.convert(jobInstance, jobParameters), jobCollectionName);

        return jobInstance;
    }

    private void validateJobInstance(String jobName, JobParameters jobParameters) {
        Assert.hasLength(jobName, "Job name must not be null or empty.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
    }

    /**
     * Create a new {@link JobExecution} based upon the {@link JobInstance} it's associated
     * with, the {@link JobParameters} used to execute it with and the location of the configuration
     * file that defines the job.  Generates a new JobExecutionId.
     *
     * @param jobInstance          {@link JobInstance} instance to initialize the new JobExecution.
     * @param jobParameters        {@link JobParameters} instance to initialize the new JobExecution.
     * @param jobConfigurationName {@link String} instance to initialize the new JobExecution.
     * @return the new {@link JobExecution}.
     * @throws IllegalArgumentException if jobInstance, jobParameters, or jobInstanceId is null.
     */
    @Override
    public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationName) {
        Assert.notNull(jobInstance, "A JobInstance is required to associate the JobExecution with");
        Assert.notNull(jobParameters, "A JobParameters object is required to create a JobExecution");
        Assert.notNull(jobInstance.getId(), "A jobInstanceId is required to create a JobExecution");

        JobExecution jobExecution = new JobExecution(jobInstance, jobParameters, jobConfigurationName);
        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        return insertNewJobExecution(jobExecution);
    }

    /**
     * <p>
     * Create a {@link JobExecution} for a given {@link Job} and
     * {@link JobParameters}. If matching {@link JobInstance} already exists,
     * the job must be restartable and it's last JobExecution must *not* be
     * completed. If matching {@link JobInstance} does not exist yet it will be
     * created.
     * </p>
     *
     * @param jobName       the name of the job that is to be executed
     * @param jobParameters the runtime parameters for the job
     * @return a valid {@link JobExecution} for the arguments provided
     * @throws JobExecutionAlreadyRunningException if there is a {@link JobExecution} already running
     *                                             for the job instance with the provided job and parameters.
     * @throws JobRestartException                 if one or more existing {@link JobInstance}s is found with
     *                                             the same parameters
     *                                             and {@link Job#isRestartable()} is false.
     * @throws JobInstanceAlreadyCompleteException if a {@link JobInstance} is
     *                                             found and was already completed successfully.
     * @throws IllegalArgumentException            if jobName or jobParameters is null/blank.
     */
    @Override
    public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
            throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        validateJobInstance(jobName, jobParameters);

        /*
         * Find all jobs matching the runtime information.
         *
         * If this method is transactional, and the isolation level is
         * REPEATABLE_READ or better, another launcher trying to start the same
         * job in another thread or process will block until this transaction
         * has finished.
         */

        var jobKey = jobKeyGenerator.generateKey(jobParameters);

        var jobExecutionDocs = mongoTemplate.find(new Query()
                        .addCriteria(Criteria.where(JOB_NAME).is(jobName))
                        .addCriteria(Criteria.where(JOB_KEY).is(jobKey))
                        .with(Sort.by(JOB_EXECUTION_ID).descending())
                , Document.class, jobCollectionName);

        if (CollectionUtils.isEmpty(jobExecutionDocs)) {
            // No JobInstance or JobExecution Found
            // Create new JobInstance and JobExecution
            var jobInstance = new JobInstance(jobInstanceCounter.nextValue(), jobName);
            JobExecution jobExecution = new JobExecution(jobInstance, jobParameters, null);
            jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

            return insertNewJobExecution(jobExecution);
        }

        // Remove JobInstance only record, if present
        jobExecutionDocs.removeIf(doc -> doc.getLong(JOB_EXECUTION_ID) == null);

        if (CollectionUtils.isEmpty(jobExecutionDocs)) {
            // JobInstance created, but has no JobExecutions
            throw new IllegalStateException("Cannot find any job execution for jobName=" + jobName + " jobKey=" + jobKey);
        }

        checkForRunningExecutions(jobExecutionDocs.stream()
                .map(JobExecutionConverter::convert).collect(Collectors.toList()));

        // build new JobExecution using JobInstance and ExecutionContext from most recent JobExecution
        var previousJobExecution = JobExecutionConverter.convert(jobExecutionDocs.get(0));
        var jobExecution = new JobExecution(previousJobExecution.getJobInstance(), jobParameters, null);
        jobExecution.setExecutionContext(previousJobExecution.getExecutionContext());
        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        return insertNewJobExecution(jobExecution);
    }

    private JobExecution insertNewJobExecution(JobExecution jobExecution) {
        jobExecution.setId(jobExecutionCounter.nextValue());
        jobExecution.incrementVersion();
        mongoTemplate.upsert(new Query()
                        .addCriteria(Criteria.where(JOB_NAME).is(jobExecution.getJobInstance().getJobName()))
                        .addCriteria(Criteria.where(JOB_KEY).is(jobKeyGenerator.generateKey(jobExecution.getJobParameters())))
                        .addCriteria(Criteria.where(JOB_EXECUTION_ID).isNull()),
                Update.fromDocument(JobExecutionConverter.convert(jobExecution)), jobCollectionName);

        return jobExecution;
    }

    @SuppressWarnings("PMD.CyclomaticComplexity")
    private void checkForRunningExecutions(Collection<JobExecution> jobExecutions)
            throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        for (var jobExecution : jobExecutions) {
            if (jobExecution.isRunning() || jobExecution.isStopping()) {
                throw new JobExecutionAlreadyRunningException("A job execution for this job is already running."
                        + " jobExecutionId=" + jobExecution.getId());
            }
            BatchStatus status = jobExecution.getStatus();
            if (status == BatchStatus.UNKNOWN) {
                throw new JobRestartException("Cannot restart job from UNKNOWN status. "
                        + "The last execution ended with a failure that could not be rolled back, "
                        + "so it may be dangerous to proceed. Manual intervention is probably necessary."
                        + " jobExecutionId=" + jobExecution.getId());
            }
            if ((status == BatchStatus.COMPLETED || status == BatchStatus.ABANDONED)
                    && hasIdentifyingParameters(jobExecution.getJobParameters())) {
                throw new JobInstanceAlreadyCompleteException(
                        "A job instance already exists and is complete."
                                + " If you want to run this job again, change the identifying parameters."
                                + " jobExecutionId=" + jobExecution.getId());
            }
        }
    }

    private boolean hasIdentifyingParameters(JobParameters jobParameters) {
        for (var jobParameter : jobParameters.getParameters().values()) {
            if (jobParameter.isIdentifying()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update the {@link JobExecution} (but not its {@link ExecutionContext}).
     * <p>
     * Preconditions: {@link JobExecution} must contain a valid
     * {@link JobInstance} and be saved (have an id assigned).
     *
     * @param jobExecution {@link JobExecution} instance to be updated in the repo.
     * @throws IllegalArgumentException if jobExecution, version, or JobExecutionId is null.
     */
    @Override
    public void update(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        synchronizeStatusAndVersion(jobExecution);

        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        updateJobExecution(jobExecution);
    }

    private void synchronizeStatusAndVersion(JobExecution jobExecution) {
        var jobExecutionSavedDoc = mongoTemplate.findOne(new Query()
                        .addCriteria(Criteria.where(JOB_NAME).is(jobExecution.getJobInstance().getJobName()))
                        .addCriteria(Criteria.where(JOB_KEY).is(jobKeyGenerator.generateKey(jobExecution.getJobParameters())))
                        .addCriteria(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId()))
                , Document.class, jobCollectionName);

        Assert.state(jobExecutionSavedDoc != null,
                () -> "Job Execution not found for jobExecutionId=" + jobExecution.getId());

        var savedJobExecution = JobExecutionConverter.convert(jobExecutionSavedDoc);

        var savedVersion = savedJobExecution.getVersion();

        if (!savedVersion.equals(jobExecution.getVersion())) {
            jobExecution.upgradeStatus(savedJobExecution.getStatus());
            jobExecution.setVersion(savedVersion);
        }
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getId(), "JobExecution must be already saved (have an id assigned).");
    }

    private void updateJobExecution(JobExecution jobExecution) {
        synchronized (jobExecution) {
            var currentVersion = jobExecution.getVersion();
            var nextVersion = currentVersion + 1;

            var updateResult = mongoTemplate.updateFirst(
                    Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId()))
                            .addCriteria(Criteria.where(VERSION).is(currentVersion)),
                    Update.update(START_TIME, jobExecution.getStartTime())
                            .set(END_TIME, jobExecution.getEndTime())
                            .set(STATUS, jobExecution.getStatus().toString())
                            .set(EXIT_CODE, jobExecution.getExitStatus().getExitCode())
                            .set(EXIT_DESCRIPTION, jobExecution.getExitStatus().getExitDescription())
                            .set(VERSION, nextVersion)
                            .set(CREATE_TIME, jobExecution.getCreateTime())
                            .set(LAST_UPDATED, jobExecution.getLastUpdated()),
                    jobCollectionName);

            if (updateResult.getModifiedCount() == 0) {
                throw new OptimisticLockingFailureException("Attempt to update job execution id="
                        + jobExecution.getId() + " with version=" + currentVersion
                        + " which was not found");
            }

            jobExecution.incrementVersion();
        }
    }

    /**
     * Persist the updated {@link ExecutionContext} of the given
     * {@link JobExecution}.
     *
     * @param jobExecution {@link JobExecution} instance to be used to update the context.
     * @throws IllegalArgumentException if jobExecution or JobExecutionId is null.
     */
    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        validateJobExecution(jobExecution);

        var updateResult = mongoTemplate.updateFirst(
                Query.query(Criteria.where(JOB_EXECUTION_ID).is(jobExecution.getId())),
                Update.update(EXECUTION_CONTEXT,
                        ExecutionContextConverter.convert(jobExecution.getExecutionContext())),
                jobCollectionName);

        Assert.state(updateResult.getMatchedCount() == 1,
                () -> "Unable to update Execution Context for missing Job Execution.  jobExecutionId="
                        + jobExecution.getId());
    }

    /**
     * @param jobName       the name of the job that might have run
     * @param jobParameters parameters identifying the {@link JobInstance}
     * @return the last execution of job if exists, null otherwise
     * @throws IllegalArgumentException if jobName or jobParameters is null/blank
     */
    @Override
    @Nullable
    public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
        validateJobInstance(jobName, jobParameters);

        var jobKey = jobKeyGenerator.generateKey(jobParameters);
        var jobExecutionDoc = mongoTemplate.findOne(new Query()
                        .addCriteria(Criteria.where(JOB_NAME).is(jobName))
                        .addCriteria(Criteria.where(JOB_KEY).is(jobKey))
                        .addCriteria(Criteria.where(JOB_EXECUTION_ID).ne(null))
                        .with(Sort.by(JOB_EXECUTION_ID).descending())
                , Document.class, jobCollectionName);
        return jobExecutionDoc == null ? null : JobExecutionConverter.convert(jobExecutionDoc);
    }

    /**
     * Save the {@link StepExecution} and its {@link ExecutionContext}. ID will
     * be assigned - it is not permitted that an ID be assigned before calling
     * this method. Instead, it should be left blank, to be assigned by a
     * {@link JobRepository}.
     * <p>
     * Preconditions: {@link StepExecution} must have a valid {@link Step}.
     *
     * @param stepExecution {@link StepExecution} instance to be added to the repo.
     * @throws IllegalArgumentException if StepExecution or jobExecutionId is null
     * @throws IllegalArgumentException if StepExecutionId is NOT null
     */
    @Override
    public void add(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");

        stepExecution.setId(stepExecutionCounter.nextValue());
        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        mongoTemplate.updateFirst(Query.query(Criteria
                        .where(JOB_EXECUTION_ID).is(stepExecution.getJobExecutionId())),
                new Update().push(STEP_EXECUTIONS, StepExecutionConverter.convert(stepExecution)),
                jobCollectionName);
    }

    /**
     * Save a collection of {@link StepExecution}s and each {@link ExecutionContext}. The
     * StepExecution ID will be assigned - it is not permitted that an ID be assigned before calling
     * this method. Instead, it should be left blank, to be assigned by {@link JobRepository}.
     * <p>
     * Preconditions: {@link StepExecution} must have a valid {@link Step}.
     *
     * @param stepExecutions collection of {@link StepExecution} instances to be added to the repo.
     * @throws IllegalArgumentException if StepExecution or jobExecutionId is null
     * @throws IllegalArgumentException if StepExecutionId is NOT null
     */
    @Override
    public void addAll(Collection<StepExecution> stepExecutions) {
        if (!CollectionUtils.isEmpty(stepExecutions)) {
            stepExecutions.forEach(this::add);
        }
    }

    /**
     * Update the {@link StepExecution} (but not its {@link ExecutionContext}).
     * <p>
     * Preconditions: {@link StepExecution} must be saved (have an id assigned).
     *
     * @param stepExecution {@link StepExecution} instance to be updated in the repo.
     * @throws IllegalArgumentException if StepExecution, StepExecutionId, or jobExecutionId is null
     */
    @Override
    public void update(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        synchronizeStatusAndVersion(stepExecution.getJobExecution());

        updateStepExecution(stepExecution);
    }

    private void updateStepExecution(StepExecution stepExecution) {
        synchronized (stepExecution.getJobExecution()) {
            var currentVersion = stepExecution.getJobExecution().getVersion();
            var nextVersion = currentVersion + 1;

            var updateResult = mongoTemplate.updateFirst(new Query()
                            .addCriteria(Criteria.where(JOB_EXECUTION_ID).is(stepExecution.getJobExecutionId()))
                            .addCriteria(Criteria.where(VERSION).is(currentVersion)),
                    new Update()
                            .set(VERSION, nextVersion)
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_START_TIME, stepExecution.getStartTime())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_END_TIME, stepExecution.getEndTime())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_STATUS, stepExecution.getStatus().toString())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_COMMIT_COUNT, stepExecution.getCommitCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_READ_COUNT, stepExecution.getReadCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_FILTER_COUNT, stepExecution.getFilterCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_WRITE_COUNT, stepExecution.getWriteCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_EXIT_CODE, stepExecution.getExitStatus().getExitCode())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_EXIT_DESCRIPTION, stepExecution.getExitStatus().getExitDescription())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_READ_SKIP_COUNT, stepExecution.getReadSkipCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_PROCESS_SKIP_COUNT, stepExecution.getProcessSkipCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_WRITE_SKIP_COUNT, stepExecution.getWriteSkipCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_ROLLBACK_COUNT, stepExecution.getRollbackCount())
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_LAST_UPDATED, stepExecution.getLastUpdated())
                            .filterArray(Criteria.where(ELEMENT_STEP_EXECUTION_ID).is(stepExecution.getId())),
                    jobCollectionName);

            if (updateResult.getMatchedCount() == 0) {
                throw new OptimisticLockingFailureException("Attempt to update job execution id="
                        + stepExecution.getJobExecution().getId() + " with version=" + currentVersion
                        + " which was not found");
            }

            stepExecution.getJobExecution().incrementVersion();
        }

        if (stepExecution.getJobExecution().isStopping()) {
            log.info("Parent JobExecution is stopped, so passing message on to StepExecution");
            stepExecution.setTerminateOnly();
        }
    }

    /**
     * Persist the updated {@link ExecutionContext}s of the given
     * {@link StepExecution}.
     *
     * @param stepExecution {@link StepExecution} instance to be used to update the context.
     * @throws IllegalArgumentException if StepExecution or jobExecutionId is null
     */
    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

        synchronized (stepExecution.getJobExecution()) {
            var executionContextDoc = ExecutionContextConverter.convert(stepExecution.getExecutionContext());

            mongoTemplate.updateFirst(new Query()
                            .addCriteria(Criteria.where(JOB_EXECUTION_ID).is(stepExecution.getJobExecutionId())),
                    new Update()
                            .set(STEP_EXECUTION_ARRAY_ELEMENT_EXECUTION_CONTEXT, executionContextDoc)
                            .filterArray(Criteria.where(ELEMENT_STEP_EXECUTION_ID).is(stepExecution.getId())),
                    jobCollectionName);
        }
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution.");
    }

    /**
     * @param jobInstance {@link JobInstance} instance containing the step executions.
     * @param stepName    the name of the step execution that might have run.
     * @return the last execution of step for the given job instance.
     */
    @Override
    @Nullable
    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        validateStepExecutionSearch(jobInstance, stepName);

        var query = newAggregation(
                match(Criteria.where(JOB_NAME).is(jobInstance.getJobName())),
                match(Criteria.where(JOB_INSTANCE_ID).is(jobInstance.getId())),
                unwind(STEP_EXECUTIONS),
                match(Criteria.where(STEP_EXECUTIONS_STEP_NAME).is(stepName)),
                sort(Sort.by(STEP_EXECUTIONS_START_TIME, STEP_EXECUTIONS_STEP_EXECUTION_ID).descending()),
                limit(1),
                lookup(jobCollectionName, JOB_EXECUTION_ID, JOB_EXECUTION_ID, JOB_EXECUTION));

        var resultDoc = mongoTemplate
                .aggregate(query, jobCollectionName, Document.class)
                .getUniqueMappedResult();

        if (CollectionUtils.isEmpty(resultDoc)) {
            return null;
        }

        var stepExecutionId = resultDoc.get(STEP_EXECUTIONS, Document.class).getLong(STEP_EXECUTION_ID);

        var jobExecutionDoc = resultDoc.getList(JOB_EXECUTION, Document.class).get(0);

        for (var stepExecution : JobExecutionConverter.convert(jobExecutionDoc).getStepExecutions()) {
            if (Objects.equals(stepExecutionId, stepExecution.getId())) {
                return stepExecution;
            }
        }

        return null;  // Not reachable
    }

    /**
     * @param jobInstance {@link JobInstance} instance containing the step executions.
     * @param stepName    the name of the step execution that might have run.
     * @return the execution count of the step within the given job instance.
     */
    @Override
    public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
        validateStepExecutionSearch(jobInstance, stepName);

        var query = newAggregation(
                match(Criteria.where(JOB_NAME).is(jobInstance.getJobName())),
                match(Criteria.where(JOB_INSTANCE_ID).is(jobInstance.getId())),
                unwind(STEP_EXECUTIONS),
                match(Criteria.where(STEP_EXECUTIONS_STEP_NAME).is(stepName)),
                count().as("steps"));

        var resultDoc = mongoTemplate
                .aggregate(query, jobCollectionName, Document.class)
                .getUniqueMappedResult();

        if (resultDoc == null) {
            return 0;
        }

        return resultDoc.getInteger("steps", 0);
    }

    private void validateStepExecutionSearch(JobInstance jobInstance, String stepName) {
        Assert.notNull(jobInstance.getId(), "jobInstanceId must not be null.");
        Assert.hasLength(stepName, "stepName must not be null or blank.");
    }
}
