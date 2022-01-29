package wirednerd;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.util.Arrays;

@Configuration
public class TestMongoClientConfiguration extends AbstractMongoClientConfiguration {

    /**
     * Return the name of the database to connect to.
     *
     * @return must not be {@literal null}.
     */
    @Override
    protected String getDatabaseName() {
        return "test";
    }

    /**
     * Configuration hook for {@link MongoCustomConversions} creation.
     *
     * @param converterConfigurationAdapter never {@literal null}.
     * @since 2.3
     */
    @Override
    protected void configureConverters(MongoCustomConversions.MongoConverterConfigurationAdapter converterConfigurationAdapter) {
        converterConfigurationAdapter.registerConverters(SpringBatchMongoConverters.converters);
    }
}
