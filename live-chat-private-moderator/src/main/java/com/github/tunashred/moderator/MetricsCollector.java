package com.github.tunashred.moderator;

import com.github.tunashred.privatedtos.ProcessedMessage;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MetricsCollector {

    static PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    static Map<String, Counter> messageCounters = new ConcurrentHashMap<>();
    static Map<String, Counter> censoredCounters = new ConcurrentHashMap<>();

    static Map<String, AtomicLong> channelMessageCounts = new ConcurrentHashMap<>();
    static Map<String, AtomicLong> userMessageCounts = new ConcurrentHashMap<>();
    static Map<String, AtomicLong> channelCensoredCounts = new ConcurrentHashMap<>();
    static Map<String, AtomicLong> userCensoredCounts = new ConcurrentHashMap<>();
    static AtomicInteger messagesCensoredTotal = new AtomicInteger(0);

    static Map<String, AtomicLong> censoredWordFrequency = new ConcurrentHashMap<>();

    static Timer messageProcessingTimer;
    static Counter processingErrorsCounter;
    static Counter totalMessagesProcessed;
    Counter totalCensoredMessages;

    Gauge activeChannelsGauge;
    Gauge activeUsersGauge;

    public MetricsCollector() {
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);

        totalMessagesProcessed = Counter.builder("messages_processed_total")
                .description("Total number of messages processed")
                .register(meterRegistry);

        Gauge.builder("messages_censored_total", messagesCensoredTotal, AtomicInteger::get)
                .description("Total number of messages censored")
                .register(meterRegistry);

        messageProcessingTimer = Timer.builder("message_processing_duration_seconds")
                .description("Time taken to process a message")
                .publishPercentileHistogram()
                .register(meterRegistry);

        processingErrorsCounter = Counter.builder("message_processing_errors_total")
                .description("Total message processing errors")
                .register(meterRegistry);

        activeChannelsGauge = Gauge.builder("active_channels_current", channelMessageCounts, Map::size)
                .description("Current number of active channels")
                .register(meterRegistry);

        activeUsersGauge = Gauge.builder("active_users_current", userMessageCounts, Map::size)
                .description("Current number of active users")
                .register(meterRegistry);

        startMetricsServer();
    }

    public static void updateMetrics(String user, String channel, ProcessedMessage result) {
        // Update basic global counters
        totalMessagesProcessed.increment();

        // Per-channel and per-user counts
        channelMessageCounts.computeIfAbsent(channel, c ->
                meterRegistry.gauge("channel_messages_total", Tags.of("channel", c), new AtomicLong(0))
        ).incrementAndGet();

        userMessageCounts.computeIfAbsent(user, u ->
                meterRegistry.gauge("user_messages_total", Tags.of("user", u), new AtomicLong(0))
        ).incrementAndGet();

        if (result.getIsModerated()) {
            messagesCensoredTotal.incrementAndGet();

            channelCensoredCounts.computeIfAbsent(channel, c ->
                    meterRegistry.gauge("channel_censored_total", Tags.of("channel", c), new AtomicLong(0))
            ).incrementAndGet();

            userCensoredCounts.computeIfAbsent(user, u ->
                    meterRegistry.gauge("user_censored_total", Tags.of("user", u), new AtomicLong(0))
            ).incrementAndGet();

            // TODO: track individual censored words
        }

        double censorshipRate = calculateCensorshipRate(channel);
        Gauge.builder("channel_censorship_rate", () -> censorshipRate)
                .tags("channel", channel)
                .register(meterRegistry);
    }

    private static double calculateCensorshipRate(String channel) {
        long total = channelMessageCounts.getOrDefault(channel, new AtomicLong(0)).get();
        long censored = channelCensoredCounts.getOrDefault(channel, new AtomicLong(0)).get();
        return total > 0 ? (double) censored / total * 100.0 : 0.0;
    }

    private void startMetricsServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(9009), 0);

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
            System.out.println("Metrics server started at http://localhost:8080/metrics");
        } catch (Exception e) {
            throw new RuntimeException("Failed to start metrics server", e);
        }
    }
}
