package wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.util.Assert;

import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

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

        document.put(STEP_EXECUTION_ID, source.getId());
        document.put(STEP_NAME, source.getStepName());
        document.put(STATUS, source.getStatus().toString());
        document.put(READ_COUNT, source.getReadCount());
        document.put(WRITE_COUNT, source.getWriteCount());
        document.put(COMMIT_COUNT, source.getCommitCount());
        document.put(ROLLBACK_COUNT, source.getRollbackCount());
        document.put(READ_SKIP_COUNT, source.getReadSkipCount());
        document.put(PROCESS_SKIP_COUNT, source.getProcessSkipCount());
        document.put(WRITE_SKIP_COUNT, source.getWriteSkipCount());
        document.put(START_TIME, source.getStartTime());
        document.put(END_TIME, source.getEndTime());
        document.put(LAST_UPDATED, source.getLastUpdated());
        document.put(EXECUTION_CONTEXT, ExecutionContextConverter.convert(source.getExecutionContext()));
        document.put(EXIT_CODE, source.getExitStatus().getExitCode());
        document.put(EXIT_DESCRIPTION, source.getExitStatus().getExitDescription());
        document.put(FILTER_COUNT, source.getFilterCount());

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

        var stepExecutionId = source.getLong(STEP_EXECUTION_ID);
        var stepName = source.getString(STEP_NAME);

        var stepExecution = new StepExecution(stepName, jobExecution, stepExecutionId);

        var status = source.getString(STATUS);
        stepExecution.setStatus(status == null ? BatchStatus.UNKNOWN : BatchStatus.valueOf(status));

        stepExecution.setReadCount(source.getInteger(READ_COUNT, 0));
        stepExecution.setWriteCount(source.getInteger(WRITE_COUNT, 0));
        stepExecution.setCommitCount(source.getInteger(COMMIT_COUNT, 0));
        stepExecution.setRollbackCount(source.getInteger(ROLLBACK_COUNT, 0));
        stepExecution.setReadSkipCount(source.getInteger(READ_SKIP_COUNT, 0));
        stepExecution.setProcessSkipCount(source.getInteger(PROCESS_SKIP_COUNT, 0));
        stepExecution.setWriteSkipCount(source.getInteger(WRITE_SKIP_COUNT, 0));
        stepExecution.setStartTime(source.getDate(START_TIME));
        stepExecution.setEndTime(source.getDate(END_TIME));
        stepExecution.setLastUpdated(source.getDate(LAST_UPDATED));
        stepExecution.setExecutionContext(ExecutionContextConverter.convert(source.get(EXECUTION_CONTEXT, Document.class)));
        stepExecution.setExitStatus(new ExitStatus(source.getString(EXIT_CODE), source.getString(EXIT_DESCRIPTION)));
        stepExecution.setFilterCount(source.getInteger(FILTER_COUNT, 0));
        return stepExecution;
    }
}
