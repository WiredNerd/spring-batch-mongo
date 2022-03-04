package io.gihub.wirednerd.mongo.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class CreateUuidList {

    @Bean
    public Job createUuidListJob(JobBuilderFactory jobs, Step uuidStep) {
        return jobs.get("CreateUuidList")
                .start(uuidStep)
                .build();
    }

    @Bean
    public Step uuidStep(StepBuilderFactory steps,
                         ItemReader<String> generateUuidValues,
                         ItemProcessor<String, String> processString,
                         ItemWriter<Object> listUuidWriter) {
        return steps.get("UuidStep")
                .<String, String>chunk(5)
                .reader(generateUuidValues)
                .processor(processString)
                .writer(listUuidWriter)
                .build();
    }

    @Bean
    public ItemReader<String> generateUuidValues() {
        return new uuidItemReader();
    }

    static class uuidItemReader implements ItemReader<String>, ItemStream {
        long processed = 0;
        long limit = 0;
        long errorOn = Long.MAX_VALUE;

        @SuppressWarnings("deprecation")
        @BeforeStep
        public void beforeStep(StepExecution stepExecution) {
            JobParameters jobParameters = stepExecution.getJobParameters();

            limit = jobParameters.getLong("limit", Long.valueOf(10));
            errorOn = jobParameters.getLong("errorOn", Long.MAX_VALUE);
        }

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
            if (executionContext.containsKey("processed")) {
                processed = executionContext.getLong("processed");
            }
        }

        @Override
        public String read() {
            if (processed >= limit) {
                return null;
            }
            if (processed == errorOn) {
                throw new RuntimeException("Failed to process row " + processed);
            }
            processed++;
            return UUID.randomUUID().toString();
        }

        @Override
        public void update(ExecutionContext executionContext) throws ItemStreamException {
            executionContext.putLong("processed", processed);
        }

        @Override
        public void close() throws ItemStreamException {
        }
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
    public ListItemWriter<Object> listUuidWriter() {
        return new ListItemWriter<>();
    }
}
