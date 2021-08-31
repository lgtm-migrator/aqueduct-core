package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.codec.ContentEncoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.model.Bootstrapable;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

@Secured("PIPE_READ")
@Measure
@Controller
@Named("controller")
public class PipeReadController implements Bootstrapable {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadController.class));
    private static final PipeLogger DEBUG_LOGGER = new PipeLogger(LoggerFactory.getLogger("pipe-debug-logger"));

    private final Reader reader;
    private final Duration bootstrapThreshold;
    private final Duration clusterChangeThreshold;
    private final ContentEncoder contentEncoder;
    private final PipeRateLimiter rateLimiter;
    private final boolean logging;
    private AtomicBoolean serviceAvailable;

    @Inject
    public PipeReadController(
        @Named("local") Reader reader,
        @Property(name = "pipe.bootstrap.threshold", defaultValue = "6h") Duration bootstrapThreshold,
        @Property(name = "pipe.clusterChange.threshold", defaultValue = "24h") Duration clusterChangeThreshold,
        @Property(name = "bootstrap.retry.logging", defaultValue = "false") boolean logging,
        ContentEncoder contentEncoder,
        PipeRateLimiter rateLimiter
    ) {
        this.reader = reader;
        this.bootstrapThreshold = bootstrapThreshold;
        this.clusterChangeThreshold = clusterChangeThreshold;
        this.logging = logging;
        this.contentEncoder = contentEncoder;
        this.rateLimiter = rateLimiter;
        this.serviceAvailable = new AtomicBoolean(true);
    }

    @Get("/pipe/{offset}{?type,location}")
    public HttpResponse<byte[]> readMessages(
        final long offset,
        final HttpRequest<?> request,
        @Nullable final List<String> type,
        @Nullable final String location
    ) {
        if (!serviceAvailable.get()) {
            return HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE);
        }

        if (offset < 0 || StringUtils.isEmpty(location)) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request);
        final List<String> types = flattenRequestParams(type);

        DEBUG_LOGGER.withLocation(location).withOffset(offset).withTypes(types).info("pipe read controller", "reading for data");

        final MessageResults messageResults = reader.read(types, offset, location);
        final List<Message> messages = messageResults.getMessages();

        final long retryAfterMs = calculateRetryAfter(messageResults);
        LOG.debug("pipe read controller", String.format("set retry time to %d", retryAfterMs));

        byte[] responseBytes = JsonHelper.toJson(messages).getBytes();

        Map<CharSequence, CharSequence> responseHeaders = new HashMap<>();

        final long retryAfterSeconds = (long) Math.ceil(retryAfterMs / (double) 1000);

        responseHeaders.put(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
        responseHeaders.put(HttpHeaders.RETRY_AFTER_MS, String.valueOf(retryAfterMs));
        responseHeaders.put(HttpHeaders.PIPE_STATE, messageResults.getPipeState().toString());

        MutableHttpResponse<byte[]> response = HttpResponse.ok(responseBytes).headers(responseHeaders);

        messageResults.getGlobalLatestOffset()
            .ifPresent(
                globalLatestOffset -> response.header(HttpHeaders.GLOBAL_LATEST_OFFSET, Long.toString(globalLatestOffset))
            );

        return response;
    }

    private long calculateRetryAfter(MessageResults messageResults) {
        if (messageResults.getMessages().isEmpty()) {
            return messageResults.getRetryAfterMs();
        }
        else if (
            isBootstrappingAndCapacityAvailable(messageResults.getMessages())
            ||
            isClusterChangeAndCapacityAvailable(messageResults.getMessages())
        ) {
            if (logging) {
                LOG.info("pipe read controller", "retry time is 0ms");
            }
            return 0;
        }

        return messageResults.getRetryAfterMs();
    }

    private boolean isClusterChangeAndCapacityAvailable(List<Message> messages) {
        return isWithinClusterChangeThreshold(messages) && rateLimiter.tryAcquire();
    }

    private boolean isWithinClusterChangeThreshold(List<Message> messages) {
        return
            messages.get(0).getCreated().isAfter(ZonedDateTime.now().minus(clusterChangeThreshold))
            &&
            messages.stream().anyMatch(message -> message.getClusterId() != message.getRoutingId());
    }

    private boolean isBootstrappingAndCapacityAvailable(List<Message> messages) {
        return isBeforeBootstrapThreshold(messages) && rateLimiter.tryAcquire();
    }

    private boolean isBeforeBootstrapThreshold(List<Message> messages) {
        return messages.get(0).getCreated().isBefore(ZonedDateTime.now().minus(bootstrapThreshold));
    }

    private void logOffsetRequestFromRemoteHost(final long offset, final HttpRequest<?> request) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(
                "pipe read controller",
                format("reading from offset %d, requested by %s", offset, request.getRemoteAddress().getHostName())
            );
        }
    }

    private List<String> flattenRequestParams(final List<String> strings) {
        if(strings == null) {
            return Collections.emptyList();
        }
        return strings
            .stream()
            .flatMap(s -> Stream.of(s.split(",")))
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        LOG.info("bootstrap", "Controller stopped");
        serviceAvailable.set(false);
    }

    @Override
    public void start() {
        LOG.info("bootstrap", "Controller started");
        serviceAvailable.set(true);
    }

    @Override
    public void reset() throws Exception {
        //null op
    }
}
