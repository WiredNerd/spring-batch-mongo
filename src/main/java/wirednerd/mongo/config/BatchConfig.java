package wirednerd.mongo.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.UUID;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Bean
    BatchConfigurer batchConfigurer(DataSource dataSource) {
        return new DefaultBatchConfigurer(dataSource);
    }

    @Bean
    public Job numberJob(JobBuilderFactory jobs, Step numberStep, Step uuidStep) {
        return jobs.get("numberJob")
                .start(numberStep)
                .next(numberStep)
                .build();
    }

    @Bean
    public Step numberStep(StepBuilderFactory steps,
                           ItemReader<Integer> generateIntegerValues,
                           ItemProcessor<Integer, String> processInteger,
                           ItemWriter<Object> listItemWriter) {
        return steps.get("NumberStep")
                .<Integer, String>chunk(5)
                .reader(generateIntegerValues)
                .processor(processInteger)
                .writer(listItemWriter)
                .build();
    }

    @Bean
    public ItemReader<Integer> generateIntegerValues() {
        return new ItemReader<Integer>() {

            long limit = 10;
            Long failAt = null;

            long value = 0;

            @BeforeStep
            public void beforeStep(StepExecution stepExecution) {
                JobParameters jobParameters = stepExecution.getJobParameters();

                limit = jobParameters.getLong("limit", Long.valueOf(10));
                failAt = jobParameters.getLong("failAt", null);
            }

            @Override
            public Integer read() throws Exception {
                if (value >= limit) {
                    return null;
                }
                if (failAt != null && value == failAt) {
                    throw new Exception("Fail At Value Reached");
                }
                return Long.valueOf(value++).intValue();
            }
        };
    }

    @Bean
    public ItemProcessor<Integer, String> processInteger() {
        return new ItemProcessor<Integer, String>() {

            Long failAt = null;

            @BeforeStep
            public void beforeStep(StepExecution stepExecution) {
                JobParameters jobParameters = stepExecution.getJobParameters();

                failAt = jobParameters.getLong("failAt", null);
            }

            @Override
            public String process(Integer item) throws Exception {
                return "Processed Integer: " + item;
            }
        };
    }

    @Bean
    public Job uuidJob(JobBuilderFactory jobs, Step numberStep, Step uuidStep) {
        return jobs.get("uuidJob")
                .start(uuidStep)
                .build();
    }

    @Bean
    public Step uuidStep(StepBuilderFactory steps,
                         ItemReader<String> generateUuidValues,
                         ItemProcessor<String, String> processString,
                         ItemWriter<Object> listItemWriter) {
        return steps.get("UuidStep")
                .<String, String>chunk(5)
                .reader(generateUuidValues)
                .processor(processString)
                .writer(listItemWriter)
                .build();
    }

    @Bean
    public ItemReader<String> generateUuidValues() {
        return new ItemReader<String>() {

            long limit = 0;

            @BeforeStep
            public void beforeStep(StepExecution stepExecution) {
                JobParameters jobParameters = stepExecution.getJobParameters();

                limit = jobParameters.getLong("limit", Long.valueOf(10));
            }

            @Override
            public String read() {
                if (limit < 0) {
                    return null;
                }
                limit--;
                return UUID.randomUUID().toString();
            }
        };
    }

    @Bean
    public ItemProcessor<String, String> processString() {
        return new ItemProcessor<>() {
            @Override
            public String process(String item) throws Exception {
                return "Processed String: " + item;
            }
        };
    }

    @Bean
    public ListItemWriter<Object> listItemWriter() {
        return new ListItemWriter<>();
    }

    @Bean
    public ItemWriter<Object> stoutItemWriter() {
        return items -> items.forEach(System.out::println);
    }


}
