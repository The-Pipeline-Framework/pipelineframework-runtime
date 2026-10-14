package org.pipelineframework.connector;

import io.smallrye.common.annotation.Blocking;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** The same raw entity shape emitted by the payload boundary compiler. */
@Path("/tpf-test/diskless-upload")
public class DisklessMultipartResource {
    @POST
    @Blocking
    @Consumes("*/*")
    public String upload(InputStream body, @HeaderParam("Content-Type") String contentType) throws IOException {
        try (var multipart = new OwnedPayloadMultipart(body, contentType)) {
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException(failure);
            }
            byte[] buffer = new byte[64 * 1024];
            int count;
            while ((count = multipart.read(buffer)) >= 0) {
                digest.update(buffer, 0, count);
            }
            return HexFormat.of().formatHex(digest.digest());
        }
    }
}
