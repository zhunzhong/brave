package brave;

import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultCurrentTraceContextTest {
  CurrentTraceContext.Default scoper = new CurrentTraceContext.Default();
  Tracer tracer = Tracer.newBuilder().build();
  TraceContext context = tracer.newTrace().context();
  TraceContext context2 = tracer.newTrace().context();

  @Test public void currentSpan_defaultsToNull() {
    assertThat(scoper.get()).isNull();
  }

  @Test public void scope_retainsContext() {
    try (CurrentTraceContext.Scope scope = scoper.newScope(context)) {
      assertThat(scoper.get())
          .isEqualTo(context);
    }
  }

  @Test public void scope_isDefinedPerThread() throws InterruptedException {
    final TraceContext[] threadValue = new TraceContext[1];

    try (CurrentTraceContext.Scope scope = scoper.newScope(context)) {
      Thread t = new Thread(() -> { // inheritable thread local
        assertThat(scoper.get())
            .isEqualTo(context);

        try (CurrentTraceContext.Scope scope2 = scoper.newScope(context2)) {
          assertThat(scoper.get())
              .isEqualTo(context2);
          threadValue[0] = context2;
        }
      });

      t.start();
      t.join();
      assertThat(scoper.get())
          .isEqualTo(context);
      assertThat(threadValue[0])
          .isEqualTo(context2);
    }
  }

  @Test public void instancesAreIndependent() {
    CurrentTraceContext.Default scoper2 = new CurrentTraceContext.Default();

    try (CurrentTraceContext.Scope scope1 = scoper.newScope(context)) {
      assertThat(scoper2.get()).isNull();

      try (CurrentTraceContext.Scope scope2 = scoper2.newScope(context2)) {
        assertThat(scoper.get()).isEqualTo(context);
        assertThat(scoper2.get()).isEqualTo(context2);
      }
    }
  }
}
