package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.http.client.HttpClient;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.reactivex.Flowable.fromIterable;

public class ServiceList {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(ServiceList.class));
    private final HttpClient httpClient;
    List<PipeServiceInstance> services;
    private final PipeServiceInstance cloudInstance;
    private final File file;
    private ZonedDateTime lastUpdatedTime;

    public ServiceList(
        HttpClient httpClient,
        final PipeServiceInstance pipeServiceInstance,
        File file
    ) throws IOException {
        this.httpClient = httpClient;
        this.cloudInstance = pipeServiceInstance;
        services = new ArrayList<>();
        lastUpdatedTime = null;
        this.file = file;
        readServicesProperties(file);
    }

    private void readServicesProperties(File file) throws IOException {
        if (!file.exists()) {
            defaultToCloud();
            return;
        }
        Properties properties = new Properties();
        try (FileInputStream stream = new FileInputStream(file)) {
            properties.load(stream);
        }
        updateServices(readUrls(properties));
        updateTime(readUpdatedTime(properties));
    }

    private List<URL> readUrls(Properties properties) {
        String urls = properties.getProperty("services", "");
        return Arrays.stream(urls.split(","))
            .map(this::toUrl)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private URL toUrl(String m) {
        try {
            return new URL(m);
        } catch (MalformedURLException e) {
            LOG.error("toUrl", "malformed url " + m, e.getMessage());
        }
        return null;
    }

    private void updateTime(ZonedDateTime zonedDateTime) {
        lastUpdatedTime = zonedDateTime;
    }

    private ZonedDateTime readUpdatedTime(Properties properties) {
        String lastRegistrationTime = properties.getProperty("lastRegistrationTime");
        return lastRegistrationTime != null ? ZonedDateTime.parse(lastRegistrationTime) : null;
    }

    public void update(final List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            defaultToCloud();
            return;
        }
        ZonedDateTime currentDateTime = ZonedDateTime.now();

        updateServices(urls);
        updateTime(currentDateTime);

        persistServiceProperties(urls, currentDateTime);
    }

    private void persistServiceProperties(List<URL> urls, ZonedDateTime currentDateTime) {
        Properties properties = new Properties();

        String urlStrings = urls.stream().map(Object::toString).collect(Collectors.joining(","));
        String registeredTime = currentDateTime.toString();

        properties.setProperty("services", urlStrings);
        properties.setProperty("lastRegistrationTime", registeredTime);

        try (OutputStream outputStream = new FileOutputStream(file)) {
            try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
                properties.store(outputStreamWriter, null);
            }
        } catch (IOException exception) {
            LOG.error("persist", "Unable to persist service urls to properties file", exception);
            throw new UncheckedIOException(exception);
        }
    }

    private void updateServices(final List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            defaultToCloud();
            return;
        }
        services = urls.stream().map(url-> url.equals(flipCloudInstanceProtocal())?cloudInstance.getUrl():url)
            .map(this::getServiceInstance)
            .collect(Collectors.toList());
        updateState();
    }

    private URL flipCloudInstanceProtocal()
    {
        //This code is added to handle envoy proxy backward compatibility to support both https and http pipe urls.
        // This has to be removed after pipe url at publisher is upgraded to http
        String httpUrl=this.cloudInstance.getUrl().toString();
        try {
            if (!httpUrl.startsWith("https")) {
                return  new URL(httpUrl.replaceFirst("^http", "https"));
            }
            else if(httpUrl.startsWith("https")) {
                return new URL(httpUrl.replaceFirst("^https", "http"));
            }
        }
        catch (Exception e)
        {
            LOG.error("persist", "Unable to parse url",e.getMessage());
        }
        return this.cloudInstance.getUrl();
    }



    private void defaultToCloud() {
        LOG.info("ServiceList.defaultToCloud", "Defaulting to follow the Cloud Pipe server.");
        List<PipeServiceInstance> justCloud = new ArrayList<>();
        justCloud.add(this.cloudInstance);
        this.services = justCloud;
    }

    private PipeServiceInstance getServiceInstance(final URL url) {
        return findPreviousInstance(url)
            .orElseGet(() -> new PipeServiceInstance(httpClient, url));
    }

    private Optional<PipeServiceInstance> findPreviousInstance(final URL url) {
        try {
            // We have to use URIs for this comparison as URLs are converted to IPs under the hood, which causes issues
            // for local testing
            final URI uri = url.toURI();
            return services.stream()
                .filter(oldInstance -> uri.equals(oldInstance.getURI()))
                .findFirst();
        } catch (URISyntaxException exception) {
            LOG.error("pipe load balancer", "invalid URI", exception);
            return Optional.empty();
        }
    }

    public void updateState() {
        fromIterable(services)
            .flatMapCompletable(PipeServiceInstance::updateState)
            .blockingAwait();
    }

    public Stream<PipeServiceInstance> stream() {
        return services.stream();
    }

    public ZonedDateTime getLastUpdatedTime() {
        return lastUpdatedTime;
    }
}
