package com.tesco.aqueduct.pipe.http.client;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;

import java.util.List;

public interface InternalHttpPipeClient {
    HttpResponse<byte[]> httpRead(
        @Nullable List<String> type,
        long offset,
        String location
    );
}
