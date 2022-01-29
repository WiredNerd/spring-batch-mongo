package wirednerd.springbatch.mongo.converter;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.Arrays;
import java.util.Collection;

public class SpringBatchMongoConverters {

    private SpringBatchMongoConverters() {
    }

    public static Collection<Converter<?, ?>> buildConverters() {
        return Arrays.asList(
                new JobParameterWriteConverter(),
                new JobParameterReadConverter(),
                new JobParametersWriteConverter(),
                new JobParametersReadConverter()
        );
    }
}
