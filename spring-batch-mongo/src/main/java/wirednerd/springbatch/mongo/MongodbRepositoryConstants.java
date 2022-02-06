package wirednerd.springbatch.mongo;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Contains static final strings used by springbatch.mongo classes
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MongodbRepositoryConstants {

    // Collections
    public static final String DEFAULT_JOB_COLLECTION = "jobExecutions";
    public static final String DEFAULT_COUNTER_COLLECTION = "counters";

    // JobExecution
    public static final String JOB_EXECUTION = "jobExecution";
    public static final String JOB_EXECUTION_ID = "jobExecutionId";
    public static final String JOB_PARAMETERS = "jobParameters";
    public static final String JOB_INSTANCE_ID = "jobInstanceId";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_KEY = "jobKey";
    public static final String STEP_EXECUTIONS = "stepExecutions";
    public static final String CREATE_TIME = "createTime";
    public static final String JOB_CONFIGURATION_NAME = "jobConfigurationName";

    // StepExecution
    public static final String STEP_EXECUTION_ID = "stepExecutionId";
    public static final String STEP_NAME = "stepName";
    public static final String READ_COUNT = "readCount";
    public static final String WRITE_COUNT = "writeCount";
    public static final String COMMIT_COUNT = "commitCount";
    public static final String ROLLBACK_COUNT = "rollbackCount";
    public static final String READ_SKIP_COUNT = "readSkipCount";
    public static final String PROCESS_SKIP_COUNT = "processSkipCount";
    public static final String WRITE_SKIP_COUNT = "writeSkipCount";
    public static final String FILTER_COUNT = "filterCount";

    // JobExecution & StepExecution
    public static final String VERSION = "version";
    public static final String STATUS = "status";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String LAST_UPDATED = "lastUpdated";
    public static final String EXIT_CODE = "exitCode";
    public static final String EXIT_DESCRIPTION = "exitDescription";
    public static final String EXECUTION_CONTEXT = "executionContext";

    // JobParameter
    public static final String IDENTIFYING = "identifying";
    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String LONG = "LONG";
    public static final String DOUBLE = "DOUBLE";

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
