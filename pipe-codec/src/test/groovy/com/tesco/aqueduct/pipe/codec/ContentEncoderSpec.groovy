package com.tesco.aqueduct.pipe.codec

import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import spock.lang.Specification

class ContentEncoderSpec extends Specification {

    def "Encode the response with brotli if brotli codec is available and requested"() {
        given: "a content encoder and working brotli codec"
        def headers = Mock (HttpHeaders) {
            contains("Accept-Encoding") >> true
            get("Accept-Encoding") >> ["br"]
        }
        def request = Mock(HttpRequest) {
            getHeaders() >> headers
        }
        def responseBytes = "test" as byte[]
        def gzipCodec = new GzipCodec(1, false)
        def brotliCodec = new BrotliCodec(1, false)
        def contentEncoder = new ContentEncoder(3, brotliCodec, gzipCodec)

        when: "we encode response"
        def response = contentEncoder.encodeResponse(request, responseBytes)

        then: "response is brotli encoded"
        brotliCodec.decode(response.encodedBody) == responseBytes
    }

    def "Don't encode the response with brotli if brotli codec is not available and brotli is requested"() {
        given: "a content encoder and non working brotli codec"
        def headers = Mock (HttpHeaders) {
            contains("Accept-Encoding") >> true
            get("Accept-Encoding") >> ["br"]
        }
        def request = Mock(HttpRequest) {
            getHeaders() >> headers
        }
        def responseBytes = "test" as byte[]
        def gzipCodec = new GzipCodec(1, false)
        def brotliCodec = Mock(BrotliCodec) {
            isAvailable() >> false
        }
        def contentEncoder = new ContentEncoder(3, brotliCodec, gzipCodec)

        when: "we encode response"
        def response = contentEncoder.encodeResponse(request, responseBytes)

        then: "response is not encoded"
        response.encodedBody == responseBytes
    }

    def "Encode the response with gzip if gzip codec is available and requested"() {
        given: "a content encoder and working gzip codec"
        def headers = Mock (HttpHeaders) {
            contains("Accept-Encoding") >> true
            get("Accept-Encoding") >> ["gzip"]
        }
        def request = Mock(HttpRequest) {
            getHeaders() >> headers
        }
        def responseBytes = "test" as byte[]
        def gzipCodec = new GzipCodec(1, false)
        def brotliCodec = new BrotliCodec(1, false)
        def contentEncoder = new ContentEncoder(3, brotliCodec, gzipCodec)

        when: "we encode response"
        def response = contentEncoder.encodeResponse(request, responseBytes)

        then: "response is gzip encoded"
        gzipCodec.decode(response.encodedBody) == responseBytes
    }
}
