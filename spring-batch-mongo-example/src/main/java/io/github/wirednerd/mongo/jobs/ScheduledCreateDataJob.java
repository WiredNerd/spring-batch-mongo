package io.github.wirednerd.mongo.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class ScheduledCreateDataJob {

    @Bean
    public Job jobScheduledCreateData(JobBuilderFactory jobs, Step uuidStep) {
        return jobs.get("ScheduledCreateData")
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
    public ItemWriter<Object> stoutItemWriter() {
        return items -> items.forEach(System.out::println);
    }
}
