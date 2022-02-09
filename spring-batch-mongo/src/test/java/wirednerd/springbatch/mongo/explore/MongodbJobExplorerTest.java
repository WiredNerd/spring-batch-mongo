package wirednerd.springbatch.mongo.explore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.data.mongodb.core.query.Query;
import wirednerd.springbatch.mongo.MongoDBContainerConfig;
import wirednerd.springbatch.mongo.converter.JobExecutionConverter;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MongodbJobExplorerTest extends MongoDBContainerConfig {

    private MongodbJobExplorer explorer;

    private final String jobCollectionName = "testJobs";
    private final String counterCollectionName = "testCounters";

    private JobExecution jobExecution11, jobExecution12, jobExecution13;
    private JobExecution jobExecution21, jobExecution22;

    private List<String> jobNamesSorted;

    @BeforeEach
    void setUp() throws Exception {
        explorer = new MongodbJobExplorer(mongoTemplate, jobCollectionName);
        Set<String> jobNames = new HashSet<>();

        jobExecution11 = new JobExecution(new JobInstance(10L, "Job1"), 11L, new JobParameters(), "");
        jobExecution11.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution11.setEndTime(new Date(System.currentTimeMillis()));

        jobExecution12 = new JobExecution(new JobInstance(10L, "Job1"), 12L, new JobParameters(), "");
        jobExecution12.setStartTime(new Date(System.currentTimeMillis()));

        var paramMap = new LinkedHashMap<String, JobParameter>();
        paramMap.put("Key", new JobParameter("Value"));

        jobExecution13 = new JobExecution(new JobInstance(11L, "Job1"), 13L, new JobParameters(paramMap), "");
        jobExecution13.setStartTime(new Date(System.currentTimeMillis()));
        jobExecution13.createStepExecution("Step1").setId(1L);
        jobExecution13.createStepExecution("Step2").setId(2L);
        jobExecution13.createStepExecution("Step3").setId(3L);

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution11), jobCollectionName);
        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution12), jobCollectionName);
        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution13), jobCollectionName);
        jobNames.add("Job1");

        jobExecution21 = new JobExecution(new JobInstance(20L, "Job2"), 21L, new JobParameters(), "");
        jobExecution22 = new JobExecution(new JobInstance(20L, "Job2"), 22L, new JobParameters(), "");

        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution21), jobCollectionName);
        mongoTemplate.insert(JobExecutionConverter.convert(jobExecution22), jobCollectionName);
        jobNames.add("Job2");

        jobNamesSorted = new ArrayList<>();
        jobNamesSorted.addAll(jobNames);
        jobNamesSorted.sort(String::compareTo);
    }

    @Test
    void getJobInstance() {
        var jobInstance = explorer.getJobInstance(10L);
        assertEquals(10L, jobInstance.getId());
        assertEquals("Job1", jobInstance.getJobName());
    }

    @Test
    void getJobInstance_notFound() {
        assertNull(explorer.getJobInstance(0L));
    }

    @Test
    void getJobNames() {
        var jobNames = explorer.getJobNames();
        assertArrayEquals(jobNamesSorted.toArray(), jobNames.toArray());
    }

    @Test
    void getJobNames_none() {
        mongoTemplate.remove(new Query(), jobCollectionName);

        var jobNames = explorer.getJobNames();
        assertEquals(0, jobNames.size());
    }

    @Test
    void getJobInstanceCount() throws NoSuchJobException {
        assertEquals(2, explorer.getJobInstanceCount("Job1"));
        assertEquals(1, explorer.getJobInstanceCount("Job2"));
    }

    @Test
    void getJobInstanceCount_notFound() {
        try {
            assertEquals(2, explorer.getJobInstanceCount("Job0"));
            fail("NoSuchJobException expected");
        } catch (NoSuchJobException e) {
            assertEquals("No job instances were found for job name Job0", e.getMessage());
        }
    }

    @Test
    void getJobInstances_Job1_0_10() {
        var result = explorer.getJobInstances("Job1", 0, 10);

        assertEquals(2, result.size());
        assertEquals(11L, result.get(0).getId());
        assertEquals("Job1", result.get(0).getJobName());
        assertEquals(10L, result.get(1).getId());
        assertEquals("Job1", result.get(1).getJobName());
    }

    @Test
    void getJobInstances_Job1_1_10() {
        var result = explorer.getJobInstances("Job1", 1, 10);

        assertEquals(1, result.size());
        assertEquals(10L, result.get(0).getId());
        assertEquals("Job1", result.get(0).getJobName());
    }

    @Test
    void getJobInstances_Job1_0_1() {
        var result = explorer.getJobInstances("Job1", 0, 1);

        assertEquals(1, result.size());
        assertEquals(11L, result.get(0).getId());
        assertEquals("Job1", result.get(0).getJobName());
    }

    @Test
    void getJobInstances_Job1_2_1() {
        var result = explorer.getJobInstances("Job1", 2, 1);

        assertEquals(0, result.size());
    }

    @Test
    void getJobInstances_notFound() {
        var result = explorer.getJobInstances("Not Found", 0, 10);

        assertEquals(0, result.size());
    }

    @Test
    void getJobInstances_nullJobName() {
        var result = explorer.getJobInstances(null, 0, 10);

        assertEquals(0, result.size());
    }

    @Test
    void getLastJobInstance_Job1() {
        var result = explorer.getLastJobInstance("Job1");

        assertEquals(11L, result.getId());
        assertEquals("Job1", result.getJobName());
    }

    @Test
    void findJobInstancesByJobName_Job1_0_10() {
        var result = explorer.findJobInstancesByJobName("Job1", 0, 10);

        assertEquals(2, result.size());
        assertEquals(11L, result.get(0).getId());
        assertEquals("Job1", result.get(0).getJobName());
        assertEquals(10L, result.get(1).getId());
        assertEquals("Job1", result.get(1).getJobName());
    }

    @Test
    void findJobInstancesByJobName_starWildcard() {
        assertEquals(2, explorer.findJobInstancesByJobName("J*1", 0, 10).size());
        assertEquals(2, explorer.findJobInstancesByJobName("Job1*", 0, 10).size());
        assertEquals(0, explorer.findJobInstancesByJobName("Job0*", 0, 10).size());
    }

    @Test
    void findJobInstancesByJobName_percentWildcard() {
        assertEquals(2, explorer.findJobInstancesByJobName("J%1", 0, 10).size());
        assertEquals(2, explorer.findJobInstancesByJobName("Job1%", 0, 10).size());
        assertEquals(0, explorer.findJobInstancesByJobName("Job0%", 0, 10).size());
    }

    @Test
    void findJobInstancesByJobName_dotWildcard() {
        assertEquals(2, explorer.findJobInstancesByJobName("Jo.1", 0, 10).size());
        assertEquals(0, explorer.findJobInstancesByJobName("Jo.b1", 0, 10).size());
        assertEquals(0, explorer.findJobInstancesByJobName("Job0.", 0, 10).size());
    }

    @Test
    void getLastJobInstance_Job2() {
        var result = explorer.getLastJobInstance("Job2");

        assertEquals(20L, result.getId());
        assertEquals("Job2", result.getJobName());
    }

    @Test
    void getLastJobInstance_notFound() {
        assertNull(explorer.getLastJobInstance("Not Found"));
    }

    @Test
    void getJobExecution() {
        var result = explorer.getJobExecution(13L);

        assertEquals(13L, result.getId());
        assertEquals("Job1", result.getJobInstance().getJobName());
        assertEquals(1, result.getJobParameters().getParameters().size());
    }

    @Test
    void getJobExecution_notFound() {
        assertNull(explorer.getJobExecution(0L));
    }

    @Test
    void getJobExecutions() {
        var result = explorer.getJobExecutions(new JobInstance(10L, "Job1"));

        assertEquals(2, result.size());
        assertEquals(12L, result.get(0).getId());
        assertEquals(11L, result.get(1).getId());
    }

    @Test
    void getJobExecutions_notFound() {
        var results = explorer.getJobExecutions(new JobInstance(0L, "Job1"));

        assertEquals(0, results.size());
    }

    @Test
    void getJobExecutions_nullInput() {
        try {
            explorer.getJobExecutions(null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobInstance must not be null.", e.getMessage());
        }
    }

    @Test
    void getLastJobExecution() {
        var result = explorer.getLastJobExecution(new JobInstance(10L, "Job1"));

        assertEquals(12L, result.getId());
    }

    @Test
    void getLastJobExecution_notFound() {
        assertNull(explorer.getLastJobExecution(new JobInstance(0L, "Job1")));
    }

    @Test
    void getLastJobExecution_nullInput() {
        try {
            explorer.getLastJobExecution(null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("JobInstance must not be null.", e.getMessage());
        }
    }

    @Test
    void findRunningJobExecutions() {
        var result = explorer.findRunningJobExecutions("Job1");

        assertEquals(2, result.size());
        var idSet = result.stream().map(exe -> exe.getId()).collect(Collectors.toSet());
        assertTrue(idSet.contains(12L), idSet.toString());
        assertTrue(idSet.contains(13L), idSet.toString());
    }

    @Test
    void getStepExecution() {
        var result = explorer.getStepExecution(13L, 2L);

        assertEquals("Step2", result.getStepName());
        assertEquals(13L, result.getJobExecutionId());
    }

    @Test
    void getStepExecution_JobExecutionNotFound() {
        assertNull(explorer.getStepExecution(0L, 2L));
    }

    @Test
    void getStepExecution_noSteps() {
        assertNull(explorer.getStepExecution(11L, 2L));
    }

    @Test
    void getStepExecution_stepNotFound() {
        assertNull(explorer.getStepExecution(13L, 0L));
    }

    @Test
    void getStepExecution_nullJobExecutionId() {
        try {
            explorer.getStepExecution(null, 1L);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("jobExecutionId must not be null.", e.getMessage());
        }
    }

    @Test
    void getStepExecution_nullStepExecutionId() {
        try {
            explorer.getStepExecution(1L, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("stepExecutionId must not be null.", e.getMessage());
        }
    }
}