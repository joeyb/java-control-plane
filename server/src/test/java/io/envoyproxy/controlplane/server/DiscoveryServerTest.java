package io.envoyproxy.controlplane.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import envoy.api.v2.Cds.Cluster;
import envoy.api.v2.Discovery.DiscoveryRequest;
import envoy.api.v2.Discovery.DiscoveryResponse;
import envoy.api.v2.Eds.ClusterLoadAssignment;
import envoy.api.v2.Lds.Listener;
import envoy.api.v2.Rds.RouteConfiguration;
import envoy.api.v2.core.Base.Node;
import envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc;
import envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub;
import io.envoyproxy.controlplane.cache.ConfigWatcher;
import io.envoyproxy.controlplane.cache.Response;
import io.envoyproxy.controlplane.cache.ResponseType;
import io.envoyproxy.controlplane.cache.Watch;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import reactor.core.publisher.EmitterProcessor;

public class DiscoveryServerTest {

  private static final String CLUSTER_NAME  = "cluster0";
  private static final String LISTENER_NAME = "listener0";
  private static final String ROUTE_NAME    = "route0";

  private static final int ENDPOINT_PORT = Resources.getAvailablePort();
  private static final int LISTENER_PORT = Resources.getAvailablePort();

  private static final Node NODE = Node.newBuilder()
      .setId("test-id")
      .setCluster("test-cluster")
      .build();

  private static final Cluster CLUSTER = Resources.createCluster(true, CLUSTER_NAME);
  private static final ClusterLoadAssignment ENDPOINT = Resources.createEndpoint(CLUSTER_NAME, ENDPOINT_PORT);
  private static final Listener LISTENER = Resources.createListener(true, LISTENER_NAME, LISTENER_PORT, ROUTE_NAME);
  private static final RouteConfiguration ROUTE = Resources.createRoute(ROUTE_NAME, CLUSTER_NAME);

  @Rule
  public final GrpcServerRule grpcServer = new GrpcServerRule();

  @Test
  public void testAggregatedHandler() throws InterruptedException {
    MockConfigWatcher configWatcher = new MockConfigWatcher(false, createResponses());
    DiscoveryServer server = new DiscoveryServer(configWatcher);

    grpcServer.getServiceRegistry().addService(server.getAggregatedDiscoveryServiceImpl());

    AggregatedDiscoveryServiceStub stub = AggregatedDiscoveryServiceGrpc.newStub(grpcServer.getChannel());
    CountDownLatch completedLatch = new CountDownLatch(1);
    AtomicBoolean error = new AtomicBoolean();

    StreamObserver<DiscoveryRequest> requestObserver = stub.streamAggregatedResources(
        new StreamObserver<DiscoveryResponse>() {
          @Override
          public void onNext(DiscoveryResponse value) {

          }

          @Override
          public void onError(Throwable t) {
            error.set(true);
          }

          @Override
          public void onCompleted() {
            completedLatch.countDown();
          }
        });

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(DiscoveryServer.LISTENER_TYPE)
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(DiscoveryServer.CLUSTER_TYPE)
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(DiscoveryServer.ENDPOINT_TYPE)
        .addResourceNames(CLUSTER_NAME)
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(DiscoveryServer.ROUTE_TYPE)
        .addResourceNames(ROUTE_NAME)
        .build());

    requestObserver.onCompleted();

    if (!completedLatch.await(1, TimeUnit.SECONDS) || error.get()) {
      fail(String.format("failed to complete request before timeout, error = %b", error.get()));
    }

    for (ResponseType type : ResponseType.values()) {
      assertThat(configWatcher.counts).containsEntry(type, 1);
    }

    assertThat(configWatcher.counts).hasSize(ResponseType.values().length);
  }

  private static Multimap<ResponseType, Response> createResponses() {
    final String version = "1";

    return ImmutableMultimap.<ResponseType, Response>builder()
        .put(ResponseType.CLUSTER_RESPONSE, Response.create(false, ImmutableList.of(CLUSTER), version))
        .put(ResponseType.ENDPOINT_RESPONSE, Response.create(false, ImmutableList.of(ENDPOINT), version))
        .put(ResponseType.LISTENER_RESPONSE, Response.create(false, ImmutableList.of(LISTENER), version))
        .put(ResponseType.ROUTE_RESPONSE, Response.create(false, ImmutableList.of(ROUTE), version))
        .build();
  }

  private static class MockConfigWatcher implements ConfigWatcher {

    private final boolean closeWatch;
    private final Map<ResponseType, Integer> counts;
    private final LinkedListMultimap<ResponseType, Response> responses;

    MockConfigWatcher(boolean closeWatch, Multimap<ResponseType, Response> responses) {
      this.closeWatch = closeWatch;
      this.counts = new HashMap<>();
      this.responses = LinkedListMultimap.create(responses);
    }

    @Override
    public Watch watch(ResponseType type, Node node, String version, Collection<String> names) {
      counts.put(type, counts.getOrDefault(type, 0) + 1);

      Watch watch = new Watch(names, type);

      if (responses.get(type).size() > 0) {
        Response response = responses.get(type).remove(0);

        EmitterProcessor<Response> emitter = (EmitterProcessor<Response>) watch.value();

        emitter.onNext(response);
      } else if (closeWatch) {
        watch.cancel();
      }

      return watch;
    }
  }
}
