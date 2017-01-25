package brave.resteasy3;

import brave.http.ITHttpClient;
import brave.jaxrs2.BraveTracingClientFilter;
import brave.jaxrs2.BraveTracingFeature;
import brave.parser.Parser;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.InvocationCallback;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

public class ITBraveTracingFeature_Client extends ITHttpClient<ResteasyClient> {

  ExecutorService executor = Executors.newSingleThreadExecutor();

  @Override protected ResteasyClient newClient(int port) {
    return configureClient(BraveTracingFeature.create(tracer));
  }

  ResteasyClient configureClient(BraveTracingFeature feature) {
    return new ResteasyClientBuilder()
        .socketTimeout(1, TimeUnit.SECONDS)
        .establishConnectionTimeout(1, TimeUnit.SECONDS)
        // TODO make a executor wrapper
        //.asyncExecutor(BraveExecutorService.wrap(executor, brave))
        .register(feature)
        .build();
  }

  @Override protected ResteasyClient newClient(int port, Supplier<String> spanNamer) {
    return configureClient(BraveTracingFeature.builder(tracer)
        .clientConfig(new BraveTracingClientFilter.Config() {
          @Override protected Parser<ClientRequestContext, String> spanNameParser() {
            return ctx -> spanNamer.get();
          }
        }).build());
  }

  @Override protected void closeClient(ResteasyClient client) throws IOException {
    if (client != null) client.close();
    executor.shutdownNow();
  }

  @Override protected void get(ResteasyClient client, String pathIncludingQuery)
      throws IOException {
    client.target(server.url(pathIncludingQuery).uri()).request().buildGet().invoke().close();
  }

  @Override protected void getAsync(ResteasyClient client, String pathIncludingQuery) {
    client.target(server.url(pathIncludingQuery).uri()).request().async().get(
        new InvocationCallback<Void>() {
          @Override public void completed(Void o) {
          }

          @Override public void failed(Throwable throwable) {
            throwable.printStackTrace();
          }
        });
  }

  @Override
  public void usesParentFromInvocationTime() throws Exception {
    throw new AssumptionViolatedException("TODO: async executor");
  }

  @Override
  @Test(expected = AssertionError.class) // doesn't yet close a span on exception
  public void reportsSpanOnTransportException() throws Exception {
    super.reportsSpanOnTransportException();
  }

  @Override
  @Test(expected = AssertionError.class) // doesn't yet close a span on exception
  public void addsErrorTagOnTransportException() throws Exception {
    super.addsErrorTagOnTransportException();
  }
}
