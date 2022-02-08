package wirednerd.springbatch.mongo.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.util.Assert;

import static wirednerd.springbatch.mongo.MongodbRepositoryConstants.*;

/**
 * Utility Class for converting objects of type {@link JobInstance} to and from {@link Document}
 *
 * @author Peter Busch
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobInstanceConverter {

    private static JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    /**
     * Convert the source object of type {@link JobInstance} to target type {@link Document}.
     * Result should be readable as a JobExecution with only jobInstanceId, jobName, and jobKey.
     *
     * @param source        the source object to convert, which must be an instance of {@link JobInstance} (never {@code null})
     * @param jobParameters are used for generating the jobKey, which is part of the stored Job Instance data.
     * @return the converted object, which must be an instance of {@link Document} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static Document convert(final JobInstance source, final JobParameters jobParameters) {
        Document document = new Document();

        Assert.notNull(source, "jobInstance must not be null");
        Assert.notNull(source.getId(), "jobInstanceId must not be null");
        Assert.notNull(jobParameters, "jobParameters must not be null");

        document.put(JOB_INSTANCE_ID, source.getId());
        document.put(JOB_NAME, source.getJobName());
        document.put(JOB_KEY, jobKeyGenerator.generateKey(jobParameters));

        return document;
    }

    /**
     * Convert the source object of type {@link Document} to target type {@link JobInstance}.
     *
     * @param source the source object to convert, which must be an instance of {@link Document} (never {@code null})
     * @return the converted object, which must be an instance of {@link JobInstance} (potentially {@code null})
     * @throws IllegalArgumentException if the source cannot be converted to the desired target type
     */
    public static JobInstance convert(final Document source) {
        return new JobInstance(source.getLong(JOB_INSTANCE_ID), source.getString(JOB_NAME));
    }
}
