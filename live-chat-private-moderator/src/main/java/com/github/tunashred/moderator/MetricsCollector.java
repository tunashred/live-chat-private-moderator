package com.github.tunashred.moderator;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

import java.net.InetSocketAddress;

@Log4j2
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MetricsCollector {
    static PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    static Timer messageProcessingTimer;
    static Counter processingErrorsCounter;

    public MetricsCollector() {
        new ProcessorMetrics().bindTo(meterRegistry);

        messageProcessingTimer = Timer.builder("message_processing_duration_seconds")
                .description("Time taken to process a message")
                .publishPercentileHistogram()
                .register(meterRegistry);

        processingErrorsCounter = Counter.builder("message_processing_errors_total")
                .description("Total message processing errors")
                .register(meterRegistry);

        startMetricsServer();
    }

    public static void recordMessage(String topic, String userId, boolean wasCensored) {
        Counter.builder("messages_total")
                .description("Total number of messages sent by user per topic")
                .tags("topic", topic, "user_id", userId)
                .register(meterRegistry)
                .increment();

        if (wasCensored) {
            Counter.builder("censored_messages_total")
                    .description("Total censored messages sent to topic")
                    .tags("topic", topic)
                    .register(meterRegistry)
                    .increment();

            Counter.builder("user_censored_messages_total")
                    .description("Total censored messages by user")
                    .tags("user_id", userId)
                    .register(meterRegistry)
                    .increment();
        }
    }

    private void startMetricsServer() {
        int port = 9009;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext("/metrics", exchange -> {
                String response = meterRegistry.scrape();
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                exchange.getResponseBody().write(response.getBytes());
                exchange.close();
            });

            server.createContext("/health", exchange -> {
                String response = "OK";
                exchange.sendResponseHeaders(200, response.getBytes().length);
                exchange.getResponseBody().write(response.getBytes());
                exchange.close();
            });

            server.start();
            log.info("Server started on port {}", port);
        } catch (Exception e) {
            log.error(e);
        }
    }
}