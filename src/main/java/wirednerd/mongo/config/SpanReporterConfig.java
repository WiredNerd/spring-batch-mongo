package wirednerd.mongo.config;

import brave.Tag;
import brave.handler.SpanHandler;
import brave.sampler.Sampler;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.cloud.sleuth.autoconfig.instrument.web.SleuthWebProperties;
import org.springframework.cloud.sleuth.autoconfig.zipkin2.ZipkinAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.ZipkinSpanHandler;

import java.util.List;
import java.util.Optional;

@Configuration
@AutoConfigureOrder(SleuthWebProperties.TRACING_FILTER_ORDER + 1)
public class SpanReporterConfig {

    @Bean
    Reporter<Span> spanReporter() {
        return new SpanReporter();
    }

    @Bean
    public SpanHandler spanHandler(Reporter<Span> spanReporter, @Nullable Tag<Throwable> errorTag) {
        ZipkinSpanHandler.Builder builder = ZipkinSpanHandler.newBuilder(spanReporter);
        Optional.ofNullable(errorTag).ifPresent(builder::errorTag);
        return builder.build();
    }

    @Bean
    public Sampler defaultSampler() {
        return Sampler.ALWAYS_SAMPLE;
    }

    @Bean(ZipkinAutoConfiguration.REPORTER_BEAN_NAME)
    Reporter<Span> myReporter() {
        return AsyncReporter.create(mySender());
    }

    @Bean(ZipkinAutoConfiguration.SENDER_BEAN_NAME)
    Sender mySender() {
        return new Sender() {
            @Override
            public Encoding encoding() {
                return Encoding.JSON;
            }

            @Override
            public int messageMaxBytes() {
                return Integer.MAX_VALUE;
            }

            @Override
            public int messageSizeInBytes(List<byte[]> encodedSpans) {
                return encoding().listSizeInBytes(encodedSpans);
            }

            @Override
            public Call<Void> sendSpans(List<byte[]> encodedSpans) {
                return Call.create(null);
            }
        };
    }
}
