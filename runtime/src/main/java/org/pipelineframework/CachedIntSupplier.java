package org.pipelineframework;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.IntSupplier;

/**
 * Caches generated metadata only for matching local execution. Remote execution
 * resolves terminal position at its pinned typed worker.
 */
final class CachedIntSupplier implements IntSupplier {

  private final IntSupplier resolver;
  private volatile OptionalInt cached = OptionalInt.empty();

  CachedIntSupplier(IntSupplier resolver) {
    this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
  }

  @Override
  public int getAsInt() {
    OptionalInt current = cached;
    if (current.isPresent()) {
      return current.getAsInt();
    }
    synchronized (this) {
      current = cached;
      if (current.isPresent()) {
        return current.getAsInt();
      }
      int resolved = resolver.getAsInt();
      if (resolved <= 0) {
        throw new IllegalStateException("Generated pipeline step count must be positive");
      }
      cached = OptionalInt.of(resolved);
      return resolved;
    }
  }
}

