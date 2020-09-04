package com.tesco.aqueduct.pipe.codec;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.dec.BrotliInputStream;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.context.annotation.Value;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@ChannelHandler.Sharable
@Singleton
public class BrotliCodec extends MessageToByteEncoder<ByteBuf> implements Codec {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(BrotliCodec.class));

    private final Encoder.Parameters parameters;

    /**
     * Allow to set compression level. It has not been tested yet on real data.
     *
     * @param qualityLevel Compression level
     */
    public BrotliCodec(@Value("${http.codec.brotli.level:4}") int qualityLevel) {
        loadBrotli();
        this.parameters = new Encoder.Parameters().setQuality(qualityLevel);
    }

    public BrotliCodec() {
        loadBrotli();
        this.parameters = new Encoder.Parameters().setQuality(4);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
        out.writeBytes(encode(msg.array()));
    }

    private void loadBrotli() {
        LOG.info("Codec", "Load Brotli: " + BrotliLoader.isBrotliAvailable());
    }

    @Override
    public byte[] encode(byte[] input) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BrotliOutputStream brotliOutputStream =
                 new BrotliOutputStream(outputStream, new Encoder.Parameters().setQuality(4))) {
            brotliOutputStream.write(input);
        } catch (IOException ioException) {
            LOG.error("Codec", "Error encoding content", ioException);
            throw new PipeCodecException("Error encoding content", ioException);
        }
        final byte[] encodedBytes = outputStream.toByteArray();
        // TODO probably not required for production
        LOG.info("Codec", "Compression ratio: " + ((double)input.length)/((double)encodedBytes.length));
        return encodedBytes;
    }

    @Override
    public byte[] decode(byte[] input) {
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final BrotliInputStream brotliInputStream = new BrotliInputStream(new ByteArrayInputStream(input))) {

            int decodedByte=brotliInputStream.read();
            while (decodedByte != -1) {
                byteArrayOutputStream.write(decodedByte);
                decodedByte = brotliInputStream.read();
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException ioException) {
            LOG.error("Codec", "Error decoding bytes.", ioException);
            throw new PipeCodecException("Error decoding bytes.", ioException);
        }
    }

    @Override
    public CodecType getType() {
        return CodecType.BROTLI;
    }
}

