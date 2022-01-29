package wirednerd.mongo.config;

import lombok.extern.slf4j.Slf4j;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

@Slf4j
public class SpanReporter implements Reporter<Span> {
    @Override
    public void report(Span span) {
        if (span == null) {
            return;
        }
        var message = new StringBuilder();
        message.append("SPAN_NAME=\"").append(span.name()).append("\" ");
        message.append("SPAN_MILLIS=\"").append(span.duration() / 1000L).append("\" ");
        span.tags().forEach((key, value) -> message.append(key).append("=\"").append(value).append("\" "));

        log.info(message.toString());
    }
}
