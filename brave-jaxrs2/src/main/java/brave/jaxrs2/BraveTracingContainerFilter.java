package brave.jaxrs2;

import brave.ServerHandler;
import brave.Span;
import brave.Tracer;
import brave.parser.Parser;
import brave.parser.TagsParser;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.io.IOException;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import zipkin.TraceKeys;

import static com.github.kristofa.brave.internal.Util.checkNotNull;

@Provider
@PreMatching
@Priority(0)
public class BraveTracingContainerFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  /** Creates a tracing interceptor with defaults. Use {@link #builder(Tracer)} to customize. */
  public static BraveTracingContainerFilter create(Tracer tracer) {
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

    public BraveTracingContainerFilter build() {
      return new BraveTracingContainerFilter(this);
    }
  }

  // Not final so it can be overridden to customize tags
  public static class Config
      extends ServerHandler.Config<ContainerRequestContext, ContainerResponseContext> {

    @Override protected Parser<ContainerRequestContext, String> spanNameParser() {
      return ContainerRequestContext::getMethod;
    }

    @Override protected TagsParser<ContainerRequestContext> requestTagsParser() {
      return (req, span) -> span.tag(TraceKeys.HTTP_URL, req.getUriInfo().toString());
    }

    @Override protected TagsParser<ContainerResponseContext> responseTagsParser() {
      return (res, span) -> {
        int httpStatus = res.getStatus();
        if (httpStatus < 200 || httpStatus > 299) {
          span.tag(TraceKeys.HTTP_STATUS_CODE, String.valueOf(httpStatus));
        }
      };
    }
  }

  final Tracer tracer;
  final ServerHandler<ContainerRequestContext, ContainerResponseContext> serverHandler;
  final TraceContext.Extractor<ContainerRequestContext> contextExtractor;

  BraveTracingContainerFilter(Builder builder) {
    tracer = builder.tracer;
    serverHandler = ServerHandler.create(builder.config);
    contextExtractor = Propagation.B3_STRING.extractor(ContainerRequestContext::getHeaderString);
  }

  @Override public void filter(ContainerRequestContext context) throws IOException {
    TraceContextOrSamplingFlags contextOrFlags = contextExtractor.extract(context);
    Span span = contextOrFlags.context() != null
        ? tracer.joinSpan(contextOrFlags.context())
        : tracer.newTrace(contextOrFlags.samplingFlags());
    try {
      serverHandler.handleReceive(context, span);
      context.setProperty(Span.class.getName(), span);
    } catch (RuntimeException e) {
      throw serverHandler.handleError(e, span);
    }
  }

  @Override
  public void filter(final ContainerRequestContext request, ContainerResponseContext response)
      throws IOException {
    Span span = (Span) request.getProperty(Span.class.getName());
    try {
      serverHandler.handleSend(response, span);
    } catch (RuntimeException e) {
      throw serverHandler.handleError(e, span);
    }
  }
}