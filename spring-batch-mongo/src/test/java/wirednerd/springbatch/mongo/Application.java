package wirednerd.springbatch.mongo;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
public class Application {

    @Bean
    BatchConfigurer batchConfigurer() {
        return new BatchConfigurer() {
            @Override
            public JobRepository getJobRepository() throws Exception {
                return null;
            }

            @Override
            public PlatformTransactionManager getTransactionManager() throws Exception {
                return null;
            }

            @Override
            public JobLauncher getJobLauncher() throws Exception {
                return null;
            }

            @Override
            public JobExplorer getJobExplorer() throws Exception {
                return null;
            }
        };
    }
}
