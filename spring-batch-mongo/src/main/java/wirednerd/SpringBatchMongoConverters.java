package wirednerd;

import java.util.Arrays;
import java.util.Collection;

public class SpringBatchMongoConverters {

    private SpringBatchMongoConverters() {
    }

    public static Collection<?> converters = Arrays.asList(
            new JobParameterWriteConverter(),
            new JobParameterReadConverter());
}
