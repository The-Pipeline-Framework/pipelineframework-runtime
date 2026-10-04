package org.pipelineframework.paging;

import java.util.Objects;
import java.util.Optional;

/** JSON frame for a REST page stream; exactly one variant is present. */
public record RemotePageWireFrame<T>(
    Optional<T> item,
    Optional<PagedSourceCompletion> completion) {
  public RemotePageWireFrame {
    item = Objects.requireNonNull(item, "item must not be null");
    completion = Objects.requireNonNull(completion, "completion must not be null");
    if (item.isPresent() == completion.isPresent()) {
      throw new IllegalArgumentException("remote page frame requires exactly one payload");
    }
  }

  public static <T> RemotePageWireFrame<T> item(T value) {
    return new RemotePageWireFrame<>(Optional.of(value), Optional.empty());
  }

  public static <T> RemotePageWireFrame<T> completion(PagedSourceCompletion value) {
    return new RemotePageWireFrame<>(Optional.empty(), Optional.of(value));
  }
}
