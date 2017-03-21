package brave.propagation;

import brave.internal.Nullable;
import java.io.Closeable;

/**
 * This makes a given span the current span by placing it in scope (usually but not always a thread
 * local scope).
 *
 * <p>This type is an SPI, and intended to be used by implementors looking to change thread-local
 * storage, or integrate with other contexts such as logging (MDC).
 *
 * <h3>Design</h3>
 *
 * This design was inspired by com.google.instrumentation.trace.ContextSpanHandler,
 * com.google.inject.servlet.RequestScoper and com.github.kristofa.brave.CurrentSpan
 */
public interface CurrentTraceContext {
  /** Returns the current span in scope or null if there isn't one. */
  @Nullable TraceContext get();

  /**
   * Sets the current span in scope until the returned object is closed. It is a programming
   * error to drop or never close the result. Using try-with-resources is preferred for this reason.
   */
  Scope newScope(TraceContext currentSpan);

  /** A span remains in the scope it was bound to until close is called. */
  interface Scope extends Closeable {
    /** No exceptions are thrown when unbinding a span scope. */
    @Override void close();
  }

  /** Default implementation which is backed by an inheritable thread local */
  final class Default implements CurrentTraceContext {
    final InheritableThreadLocal<TraceContext> local = new InheritableThreadLocal<>();

    @Override public TraceContext get() {
      return local.get();
    }

    @Override public Scope newScope(TraceContext currentSpan) {
      final TraceContext previous = local.get();
      local.set(currentSpan);
      return () -> local.set(previous);
    }
  }
}