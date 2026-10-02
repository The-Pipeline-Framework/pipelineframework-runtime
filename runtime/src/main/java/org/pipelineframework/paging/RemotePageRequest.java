package org.pipelineframework.paging;

import java.util.Objects;
import java.util.Optional;

/** JSON request for a REST source-host page stream. */
public record RemotePageRequest<T>(
    T input,
    String sourceIdentity,
    Optional<String> startCheckpoint,
    int maxRecords,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String catalogFingerprint) {
  public RemotePageRequest {
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(sourceIdentity, "sourceIdentity must not be null");
    startCheckpoint = Objects.requireNonNull(startCheckpoint, "startCheckpoint must not be null");
    if (sourceIdentity.isBlank() || maxRecords < 1) {
      throw new IllegalArgumentException("remote page requires a source identity and positive record limit");
    }
    for (String value : new String[] { pipelineId, contractVersion, releaseVersion, catalogFingerprint }) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("remote page requires a pinned release identity");
      }
    }
  }
}
