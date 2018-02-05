package io.envoyproxy.controlplane.server;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.protobuf.Any;
import envoy.api.v2.ClusterDiscoveryServiceGrpc.ClusterDiscoveryServiceImplBase;
import envoy.api.v2.Discovery.DiscoveryRequest;
import envoy.api.v2.Discovery.DiscoveryResponse;
import envoy.api.v2.EndpointDiscoveryServiceGrpc.EndpointDiscoveryServiceImplBase;
import envoy.api.v2.ListenerDiscoveryServiceGrpc.ListenerDiscoveryServiceImplBase;
import envoy.api.v2.RouteDiscoveryServiceGrpc.RouteDiscoveryServiceImplBase;
import envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase;
import io.envoyproxy.controlplane.cache.ConfigWatcher;
import io.envoyproxy.controlplane.cache.Response;
import io.envoyproxy.controlplane.cache.ResponseType;
import io.envoyproxy.controlplane.cache.Watch;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class DiscoveryServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryServer.class);

  private static final String TYPE_PREFIX = "type.googleapis.com/envoy.api.v2.";

  private static final String ANY_TYPE = "";

  static final String CLUSTER_TYPE  = TYPE_PREFIX + "Cluster";
  static final String ENDPOINT_TYPE = TYPE_PREFIX + "ClusterLoadAssignment";
  static final String LISTENER_TYPE = TYPE_PREFIX + "Listener";
  static final String ROUTE_TYPE    = TYPE_PREFIX + "RouteConfiguration";

  private final ConfigWatcher configWatcher;
  private final AtomicLong streamCount = new AtomicLong();

  public DiscoveryServer(ConfigWatcher configWatcher) {
    this.configWatcher = configWatcher;
  }

  /**
   * Returns an ADS implementation that uses this server's {@link ConfigWatcher}.
   */
  public AggregatedDiscoveryServiceImplBase getAggregatedDiscoveryServiceImpl() {
    return new AggregatedDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamAggregatedResources(
          StreamObserver<DiscoveryResponse> responseObserver) {

        return createRequestHandler(responseObserver, ANY_TYPE);
      }
    };
  }

  /**
   * Returns a CDS implementation that uses this server's {@link ConfigWatcher}.
   */
  public ClusterDiscoveryServiceImplBase getClusterDiscoveryServiceImpl() {
    return new ClusterDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamClusters(
          StreamObserver<DiscoveryResponse> responseObserver) {

        return createRequestHandler(responseObserver, CLUSTER_TYPE);
      }
    };
  }

  /**
   * Returns an EDS implementation that uses this server's {@link ConfigWatcher}.
   */
  public EndpointDiscoveryServiceImplBase getEndpointDiscoveryServiceImpl() {
    return new EndpointDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamEndpoints(
          StreamObserver<DiscoveryResponse> responseObserver) {

        return createRequestHandler(responseObserver, ENDPOINT_TYPE);
      }
    };
  }

  /**
   * Returns a LDS implementation that uses this server's {@link ConfigWatcher}.
   */
  public ListenerDiscoveryServiceImplBase getListenerDiscoveryServiceImpl() {
    return new ListenerDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamListeners(
          StreamObserver<DiscoveryResponse> responseObserver) {

        return createRequestHandler(responseObserver, LISTENER_TYPE);
      }
    };
  }

  /**
   * Returns a RDS implementation that uses this server's {@link ConfigWatcher}.
   */
  public RouteDiscoveryServiceImplBase getRouteDiscoveryServiceImpl() {
    return new RouteDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamRoutes(
          StreamObserver<DiscoveryResponse> responseObserver) {

        return createRequestHandler(responseObserver, ROUTE_TYPE);
      }
    };
  }

  private StreamObserver<DiscoveryRequest> createRequestHandler(
      StreamObserver<DiscoveryResponse> responseObserver,
      String defaultTypeUrl) {

    long streamId = streamCount.getAndIncrement();

    LOGGER.info("[{}] open stream from {}", streamId, defaultTypeUrl);

    return new StreamObserver<DiscoveryRequest>() {

      private Watch clusters = null;
      private Watch endpoints = null;
      private Watch listeners = null;
      private Watch routes = null;

      private AtomicReference<String> clusterNonce = new AtomicReference<>();
      private AtomicReference<String> endpointNonce = new AtomicReference<>();
      private AtomicReference<String> listenerNonce = new AtomicReference<>();
      private AtomicReference<String> routeNonce = new AtomicReference<>();

      private AtomicLong streamNonce = new AtomicLong();

      @Override
      public void onNext(DiscoveryRequest request) {
        String nonce = request.getResponseNonce();
        String typeUrl = request.getTypeUrl();

        if (defaultTypeUrl.equals(ANY_TYPE)) {
          if (typeUrl.isEmpty()) {
            responseObserver.onError(
                Status.UNKNOWN.withDescription("type URL is required for ADS").asRuntimeException());
          }
        } else if (typeUrl.isEmpty()) {
          typeUrl = defaultTypeUrl;
        }

        LOGGER.info("[{}] request {}[{}] with nonce {} from version {}",
            streamId,
            typeUrl,
            String.join(", ", request.getResourceNamesList()),
            nonce,
            request.getVersionInfo());

        if (typeUrl.equals(CLUSTER_TYPE) && test(clusterNonce, n -> isNullOrEmpty(n) || n.equals(nonce))) {
          if (clusters != null) {
            clusters.cancel();
          }

          clusters = configWatcher.watch(
              ResponseType.CLUSTER_RESPONSE,
              request.getNode(),
              request.getVersionInfo(),
              request.getResourceNamesList());

          Flux.from(clusters.value()).subscribe(r -> clusterNonce.set(send(r, CLUSTER_TYPE)));
        }

        if (typeUrl.equals(ENDPOINT_TYPE) && test(endpointNonce, n -> isNullOrEmpty(n) || n.equals(nonce))) {
          if (endpoints != null) {
            endpoints.cancel();
          }

          endpoints = configWatcher.watch(
              ResponseType.ENDPOINT_RESPONSE,
              request.getNode(),
              request.getVersionInfo(),
              request.getResourceNamesList());

          Flux.from(endpoints.value()).subscribe(r -> endpointNonce.set(send(r, ENDPOINT_TYPE)));
        }

        if (typeUrl.equals(LISTENER_TYPE) && test(listenerNonce, n -> isNullOrEmpty(n) || n.equals(nonce))) {
          if (listeners != null) {
            listeners.cancel();
          }

          listeners = configWatcher.watch(
              ResponseType.LISTENER_RESPONSE,
              request.getNode(),
              request.getVersionInfo(),
              request.getResourceNamesList());

          Flux.from(listeners.value()).subscribe(r -> listenerNonce.set(send(r, LISTENER_TYPE)));
        }

        if (typeUrl.equals(ROUTE_TYPE) && test(routeNonce, n -> isNullOrEmpty(n) || n.equals(nonce))) {
          if (routes != null) {
            routes.cancel();
          }

          routes = configWatcher.watch(
              ResponseType.ROUTE_RESPONSE,
              request.getNode(),
              request.getVersionInfo(),
              request.getResourceNamesList());

          Flux.from(routes.value()).subscribe(r -> routeNonce.set(send(r, ROUTE_TYPE)));
        }
      }

      @Override
      public void onError(Throwable t) {
        LOGGER.error("[{}] stream closed with error", streamId, t);
        responseObserver.onError(Status.fromThrowable(t).asException());
        cancel();
      }

      @Override
      public void onCompleted() {
        LOGGER.info("[{}] stream closed", streamId);
        responseObserver.onCompleted();
        cancel();
      }

      private void cancel() {
        execIfNotNull(clusters, Watch::cancel);
        execIfNotNull(endpoints, Watch::cancel);
        execIfNotNull(listeners, Watch::cancel);
        execIfNotNull(routes, Watch::cancel);
      }

      private String send(Response response, String typeUrl) {
        String nonce = Long.toString(streamNonce.getAndIncrement());

        DiscoveryResponse discoveryResponse = DiscoveryResponse.newBuilder()
            .setVersionInfo(response.version())
            .addAllResources(response.resources().stream().map(Any::pack).collect(Collectors.toList()))
            .setCanary(response.canary())
            .setTypeUrl(typeUrl)
            .setNonce(nonce)
            .build();

        LOGGER.info("[{}] response {} with nonce {} version {}", streamId, typeUrl, nonce, response.version());

        responseObserver.onNext(discoveryResponse);

        return nonce;
      }
    };
  }

  private static <T> void execIfNotNull(T value, Consumer<T> consumer) {
    if (value != null) {
      consumer.accept(value);
    }
  }

  private static <T> boolean test(AtomicReference<T> reference, Predicate<T> predicate) {
    return predicate.test(reference.get());
  }
}
