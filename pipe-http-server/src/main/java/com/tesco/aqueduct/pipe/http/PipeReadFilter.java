package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.codec.ContentEncoder;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.inject.Inject;

@Filter("/pipe/*")
public class PipeReadFilter implements HttpServerFilter {

    private final ContentEncoder encoder;

    @Inject
    public PipeReadFilter(final ContentEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        return Flowable.fromPublisher(chain.proceed(request))
            .doOnNext(response -> {
                if (isReadEndpoint(request) && response.status() == HttpStatus.OK) {
                    final ContentEncoder.EncodedResponse encodedResponse = encoder.encodeResponse(request, (byte[]) response.body());
                    response.body(encodedResponse.getEncodedBody());
                    encodedResponse.getHeaders().forEach(response::header);
                }
            });
    }

    private boolean isReadEndpoint(HttpRequest<?> request) {
        return request.getUri().getPath().matches("\\/pipe\\/\\d+");
    }
}