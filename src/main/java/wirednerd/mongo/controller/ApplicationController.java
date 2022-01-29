package wirednerd.mongo.controller;

import lombok.Data;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.*;

@RestController
public class ApplicationController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private Map<String, Job> jobMap;

    @Autowired
    private ListItemWriter<Object> listItemWriter;

    @GetMapping("/hello")
    public String hello() {
        return "Hello World";
    }

    @GetMapping("/job/{name}")
    public Object runJob(@PathVariable("name") String jobName,
                         @RequestParam(value = "limit", defaultValue = "10") Long limit,
                         @RequestParam(value = "failAt", required = false) Long failAt,
                         @RequestParam(value = "seed", defaultValue = "true") boolean seed
    ) throws Exception {

        var jobParametersBuilder = new JobParametersBuilder()
                .addLong("limit", limit)
                .addLong("failAt", failAt, false);
        if (seed) {
            jobParametersBuilder.addString("seed", UUID.randomUUID().toString());
        }

        jobLauncher.run(jobMap.get(jobName), jobParametersBuilder.toJobParameters());

        return listItemWriter.getWrittenItems();
    }

    @Autowired
    MongoTemplate mongoTemplate;

    @PostMapping("/jobParameter")
    public void postJobParameter(@RequestBody MongoJobParameter mongoJobParameter) {
        var doc = new TestDocument2();
        doc.setJobParameter(mongoJobParameter.toJobParameter());
        mongoTemplate.insert(doc);
    }

    @GetMapping("/jobParameter")
    public TestDocument2 getJobParameter() {
        return mongoTemplate.findOne(new Query(), TestDocument2.class);
    }

    @GetMapping("/jobParameter/example")
    public MongoJobParameter getExampleJobParameter() {
        var out = new MongoJobParameter();
        out.setIdentifying(true);
        out.setValueDate(Date.from(OffsetDateTime.now().toInstant()));
        out.setValueLong(123L);
        out.setValueDouble(1.2);
        out.setValueString("Test");
        return out;
    }

    @Data
    @Document("Test")
    class TestDocument2 {
        JobParameter jobParameter;
    }

}
