package io.github.wirednerd.springbatch.mongo;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.*;

/**
 * Contains static final strings used by springbatch.mongo classes
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MongodbRepositoryConstants {

    public static final String ID = "_id"; //NOPMD

    // Collections
    public static final String DEFAULT_JOB_COLLECTION = "jobExecutions";
    public static final String DEFAULT_COUNTER_COLLECTION = "counters";

    // JobExecution
    public static final String JOB_EXECUTION = "jobExecution";

    // StepExecution Update
    private static final String STEP_EXECUTION_ARRAY_ELEMENT = STEP_EXECUTIONS + ".$[elem].";
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_START_TIME = STEP_EXECUTION_ARRAY_ELEMENT + START_TIME;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_END_TIME = STEP_EXECUTION_ARRAY_ELEMENT + END_TIME;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_STATUS = STEP_EXECUTION_ARRAY_ELEMENT + STATUS;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_COMMIT_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + COMMIT_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_READ_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + READ_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_FILTER_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + FILTER_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_WRITE_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + WRITE_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_EXIT_CODE = STEP_EXECUTION_ARRAY_ELEMENT + EXIT_CODE;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_EXIT_DESCRIPTION = STEP_EXECUTION_ARRAY_ELEMENT + EXIT_DESCRIPTION;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_READ_SKIP_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + READ_SKIP_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_PROCESS_SKIP_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + PROCESS_SKIP_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_WRITE_SKIP_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + WRITE_SKIP_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_ROLLBACK_COUNT = STEP_EXECUTION_ARRAY_ELEMENT + ROLLBACK_COUNT;
    public static final String STEP_EXECUTION_ARRAY_ELEMENT_LAST_UPDATED = STEP_EXECUTION_ARRAY_ELEMENT + LAST_UPDATED;

    public static final String STEP_EXECUTION_ARRAY_ELEMENT_EXECUTION_CONTEXT = STEP_EXECUTION_ARRAY_ELEMENT + EXECUTION_CONTEXT;

    public static final String ELEMENT_STEP_EXECUTION_ID = "elem." + STEP_EXECUTION_ID;

    // StepExecution Search
    public static final String STEP_EXECUTIONS_STEP_EXECUTION_ID = STEP_EXECUTIONS + "." + STEP_EXECUTION_ID;
    public static final String STEP_EXECUTIONS_STEP_NAME = STEP_EXECUTIONS + "." + STEP_NAME;
    public static final String STEP_EXECUTIONS_START_TIME = STEP_EXECUTIONS + "." + START_TIME;
}
