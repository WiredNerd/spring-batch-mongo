# spring-batch-mongo

This package allows users to use a MongoDB based Job Repository with Spring Batch.

There are several other example projects that accomplish this by creating instances of the for DAO interfaces used by
Spring Batch. However, these store the job execution data across multiple collections. And there is no good way to link
the collections together. They also do not enable transaction management, which is required for spring batch to work
correctly.

This package instead implements instances of the JobRepository and JobExplorer.  
The job execution data is stored in a single collection as a document that includes job, step, parameter, and context
data. This package also includes an instance of BatchConfigurer. It configures the JobRepository, JobExplorer,
JobLauncher, TransactionManager, and required Indexes.

# Getting Started

Maven: TBD

Gradle: TBD

## Configuration Example

```java

@Configuration
@EnableBatchProcessing
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
```

The builder also allows setting custom Job Collection Name, Counter Collection Name, and TaskExecutor.

If your project is not using any jdbc data sources, you may need to exclude spring's DataSourceAutoConfiguration. There
are multiple ways to do this, here's one:

```java
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
```

## Defaults:

| Field                   | Default         |
|-------------------------|-----------------|
| mongoTemplate           | Required        |
| mongoTransactionManager | Required        |
| Job Collection Name     | "jobExecutions" |
| Counter Collection Name | "counters"      |
| taskExecutor            | null*           |

*If taskExecutor is not provided, SimpleJobLauncher will create one.

## Transaction Management

MongoDB Transaction Management is required for this library. Please ensure you are using a version of MongoDB that
supports transactions.

Helpful Links:

* [MongoDB Manual:Transactions](https://docs.mongodb.com/manual/core/transactions/)
* [Transaction numbers are only allowed on a replica set member or mongos](https://stackoverflow.com/questions/51461952/mongodb-v4-0-transaction-mongoerror-transaction-numbers-are-only-allowed-on-a)

# Document Models

## Job Execution

```json
{
  "jobInstanceId": "<long>",
  "jobName": "<string>",
  "jobKey": "<string>",
  "jobParameters": {
    "<stringParameterKey>": {
      "STRING": "<string>",
      "identifying": "<boolean, default true>"
    },
    "<dateParameterKey>": {
      "DATE": "<date>",
      "identifying": "<boolean, default true>"
    },
    "<longParameterKey>": {
      "LONG": "<long>",
      "identifying": "<boolean, default true>"
    },
    "<doubleParameterKey>": {
      "DOUBLE": "<double>",
      "identifying": "<boolean, default true>"
    }
  },
  "jobExecutionId": "<long>",
  "version": "<integer>",
  "status": "<string>",
  "startTime": "<date>",
  "createTime": "<date>",
  "endTime": "<date>",
  "lastUpdated": "<date>",
  "exitCode": "<string>",
  "exitDescription": "<string>",
  "jobConfigurationName": "<string>",
  "executionContext": {
    "<key>": "<value>",
    "<key>": "<value>"
  },
  "stepExecutions": [
    {
      "stepExecutionId": "<long>",
      "stepName": "<string>",
      "status": "<string>",
      "readCount": "<integer>",
      "writeCount": "<integer>",
      "commitCount": "<integer>",
      "rollbackCount": "<integer>",
      "readSkipCount": "<integer>",
      "processSkipCount": "<integer>",
      "writeSkipCount": "<integer>",
      "startTime": "<date>",
      "endTime": "<date>",
      "lastUpdated": "<date>",
      "exitCode": "<string>",
      "exitDescription": "<string>",
      "filterCount": "<integer>",
      "executionContext": {
        "<key>": "<value>",
        "<key>": "<value>"
      }
    }
  ]
}
```

## Job Instance

Only when there are no executions.

```json
{
  "jobInstanceId": "<long>",
  "jobName": "<string>",
  "jobKey": "<string>"
}
```

## Counter

```json
{
  "counter": "<string>",
  "value": "<long>"
}
```

## Indexes

| Collection    | Name                            | Fields                          | Properties |
|---------------|---------------------------------|---------------------------------|------------|
| counters      | counter_unique                  | counter                         | unique     |
| jobExecutions | jobInstance_jobExecution_unique | jobName, jobKey, jobExecutionId | unique     |
| jobExecutions | jobExecutionId_unique           | jobExecutionId                  | unique     |
| jobExecutions | jobInstanceId                   | jobInstanceId                   |            |
| jobExecutions | jobName_jobInstanceId           | jobName, jobInstanceId          |            |