package wirednerd.springbatch.mongo;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import wirednerd.springbatch.mongo.configuration.MongodbBatchConfigurer;

@Configuration
@EnableTransactionManagement
public class BatchConfiguration {

    @Bean
    MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
        return new MongoTransactionManager(mongoDatabaseFactory);
    }

    @Bean
    BatchConfigurer batchConfigurer(MongoTransactionManager mongoTransactionManager, MongoTemplate mongoTemplate) {
        return MongodbBatchConfigurer.builder()
                .mongoTemplate(mongoTemplate)
                .mongoTransactionManager(mongoTransactionManager)
                .build();
    }
}
