package io.github.wirednerd.springbatch.mongo.converter;

import io.github.wirednerd.springbatch.mongo.MongodbRepositoryConstants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.util.Assert;

/**
 * Utility Class for converting objects of type {@link StepExecution} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StepExecutionConverter {

    /**
     * Convert the source object of type {@link StepExecution} to target type {@link Document}.
     *
     * @param source the source object to convert, which must be an instance of {@link StepExecution} (never {@code null})
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(final StepExecution source) {
        Document document = new Document();

        Assert.notNull(source.getId(), "stepExecutionId must not be null");
        Assert.notNull(source.getStatus(), "status must not be null");
        Assert.notNull(source.getExitStatus(), "exitStatus must not be null");

        document.put(MongodbRepositoryConstants.STEP_EXECUTION_ID, source.getId());
        document.put(MongodbRepositoryConstants.STEP_NAME, source.getStepName());
        document.put(MongodbRepositoryConstants.STATUS, source.getStatus().toString());
        document.put(MongodbRepositoryConstants.READ_COUNT, source.getReadCount());
        document.put(MongodbRepositoryConstants.WRITE_COUNT, source.getWriteCount());
        document.put(MongodbRepositoryConstants.COMMIT_COUNT, source.getCommitCount());
        document.put(MongodbRepositoryConstants.ROLLBACK_COUNT, source.getRollbackCount());
        document.put(MongodbRepositoryConstants.READ_SKIP_COUNT, source.getReadSkipCount());
        document.put(MongodbRepositoryConstants.PROCESS_SKIP_COUNT, source.getProcessSkipCount());
        document.put(MongodbRepositoryConstants.WRITE_SKIP_COUNT, source.getWriteSkipCount());
        document.put(MongodbRepositoryConstants.START_TIME, source.getStartTime());
        document.put(MongodbRepositoryConstants.END_TIME, source.getEndTime());
        document.put(MongodbRepositoryConstants.LAST_UPDATED, source.getLastUpdated());
        document.put(MongodbRepositoryConstants.EXECUTION_CONTEXT, ExecutionContextConverter.serializeContext(source.getExecutionContext()));
        document.put(MongodbRepositoryConstants.EXIT_CODE, source.getExitStatus().getExitCode());
        document.put(MongodbRepositoryConstants.EXIT_DESCRIPTION, source.getExitStatus().getExitDescription());
        document.put(MongodbRepositoryConstants.FILTER_COUNT, source.getFilterCount());

        return document;
    }

    /**
     * Convert the source object of type {@link Document} to target type {@link StepExecution}.
     *
     * @param source       the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @param jobExecution is the instance of {@link JobExecution} referenced from the final {@link StepExecution}
     * @return the converted object, which must be an instance of {@link StepExecution} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static StepExecution convert(final Document source, final JobExecution jobExecution) {

        var stepExecutionId = source.getLong(MongodbRepositoryConstants.STEP_EXECUTION_ID);
        var stepName = source.getString(MongodbRepositoryConstants.STEP_NAME);

        var stepExecution = new StepExecution(stepName, jobExecution, stepExecutionId);

        var status = source.getString(MongodbRepositoryConstants.STATUS);
        stepExecution.setStatus(status == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(status));

        stepExecution.setReadCount(source.getInteger(MongodbRepositoryConstants.READ_COUNT, 0));
        stepExecution.setWriteCount(source.getInteger(MongodbRepositoryConstants.WRITE_COUNT, 0));
        stepExecution.setCommitCount(source.getInteger(MongodbRepositoryConstants.COMMIT_COUNT, 0));
        stepExecution.setRollbackCount(source.getInteger(MongodbRepositoryConstants.ROLLBACK_COUNT, 0));
        stepExecution.setReadSkipCount(source.getInteger(MongodbRepositoryConstants.READ_SKIP_COUNT, 0));
        stepExecution.setProcessSkipCount(source.getInteger(MongodbRepositoryConstants.PROCESS_SKIP_COUNT, 0));
        stepExecution.setWriteSkipCount(source.getInteger(MongodbRepositoryConstants.WRITE_SKIP_COUNT, 0));
        stepExecution.setStartTime(source.getDate(MongodbRepositoryConstants.START_TIME));
        stepExecution.setEndTime(source.getDate(MongodbRepositoryConstants.END_TIME));
        stepExecution.setLastUpdated(source.getDate(MongodbRepositoryConstants.LAST_UPDATED));
        stepExecution.setExecutionContext(ExecutionContextConverter.deserializeContext(source.getString(MongodbRepositoryConstants.EXECUTION_CONTEXT)));
        stepExecution.setExitStatus(new ExitStatus(source.getString(MongodbRepositoryConstants.EXIT_CODE), source.getString(MongodbRepositoryConstants.EXIT_DESCRIPTION)));
        stepExecution.setFilterCount(source.getInteger(MongodbRepositoryConstants.FILTER_COUNT, 0));
        return stepExecution;
    }
}
