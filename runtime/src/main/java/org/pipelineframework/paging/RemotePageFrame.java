package org.pipelineframework.paging;

import java.util.Objects;

/** One item or the terminal checkpoint on a remote paged-source stream. */
public sealed interface RemotePageFrame<T>
    permits RemotePageFrame.Item, RemotePageFrame.Completion {

  record Item<T>(T value) implements RemotePageFrame<T> {
    public Item {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  record Completion<T>(PagedSourceCompletion value) implements RemotePageFrame<T> {
    public Completion {
      Objects.requireNonNull(value, "value must not be null");
    }
  }
}
