package brave.jaxrs2;

import brave.ClientHandler;
import brave.Span;
import brave.Tracer;
import brave.parser.Parser;
import brave.parser.TagsParser;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import java.io.IOException;
import javax.annotation.Priority;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import zipkin.TraceKeys;

import static com.github.kristofa.brave.internal.Util.checkNotNull;

@Provider
@Priority(0)
public class BraveTracingClientFilter implements ClientRequestFilter, ClientResponseFilter {

  /** Creates a tracing interceptor with defaults. Use {@link #builder(Tracer)} to customize. */
  public static BraveTracingClientFilter create(Tracer tracer) {
    return builder(tracer).build();
  }

  public static Builder builder(Tracer tracer) {
    return new Builder(tracer);
  }

  public static final class Builder {
    final Tracer tracer;
    Config config = new Config();

    Builder(Tracer tracer) { // intentionally hidden
      this.tracer = checkNotNull(tracer, "tracer");
    }

    public Builder config(Config config) {
      this.config = checkNotNull(config, "config");
      return this;
    }

    public BraveTracingClientFilter build() {
      return new BraveTracingClientFilter(this);
    }
  }

  // Not final so it can be overridden to customize tags
  public static class Config
      extends ClientHandler.Config<ClientRequestContext, ClientResponseContext> {

    @Override protected Parser<ClientRequestContext, String> spanNameParser() {
      return ClientRequestContext::getMethod;
    }

    @Override protected TagsParser<ClientRequestContext> requestTagsParser() {
      return (req, span) -> span.tag(TraceKeys.HTTP_URL, req.getUri().toString());
    }

    @Override protected TagsParser<ClientResponseContext> responseTagsParser() {
      return (res, span) -> {
        int httpStatus = res.getStatus();
        if (httpStatus < 200 || httpStatus > 299) {
          span.tag(TraceKeys.HTTP_STATUS_CODE, String.valueOf(httpStatus));
        }
      };
    }
  }

  final Tracer tracer;
  final ClientHandler<ClientRequestContext, ClientResponseContext> clientHandler;
  final TraceContext.Injector<MultivaluedMap> injector;

  BraveTracingClientFilter(Builder builder) {
    tracer = builder.tracer;
    clientHandler = ClientHandler.create(builder.config);
    injector = Propagation.B3_STRING.injector(MultivaluedMap::putSingle);
  }

  @Override
  public void filter(ClientRequestContext request) throws IOException {
    Span span = tracer.nextSpan();
    request.setProperty(Span.class.getName(), span);
    clientHandler.handleSend(request, span);
    injector.inject(span.context(), request.getHeaders());
  }

  @Override
  public void filter(ClientRequestContext request, ClientResponseContext response)
      throws IOException {
    Span span = (Span) request.getProperty(Span.class.getName());
    clientHandler.handleReceive(response, span);
  }
}