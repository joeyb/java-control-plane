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
import io.envoyproxy.controlplane.cache.ResourceType;
import io.envoyproxy.controlplane.cache.Response;
import io.envoyproxy.controlplane.cache.Watch;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Condition;
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

  private static final String VERSION = Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000));

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
    Collection<DiscoveryResponse> responses = new LinkedList<>();

    StreamObserver<DiscoveryRequest> requestObserver = stub.streamAggregatedResources(
        new StreamObserver<DiscoveryResponse>() {
          @Override
          public void onNext(DiscoveryResponse value) {
            responses.add(value);
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
        .setTypeUrl(ResourceType.LISTENER.typeUrl())
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(ResourceType.CLUSTER.typeUrl())
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(ResourceType.ENDPOINT.typeUrl())
        .addResourceNames(CLUSTER_NAME)
        .build());

    requestObserver.onNext(DiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(ResourceType.ROUTE.typeUrl())
        .addResourceNames(ROUTE_NAME)
        .build());

    requestObserver.onCompleted();

    if (!completedLatch.await(1, TimeUnit.SECONDS) || error.get()) {
      fail(String.format("failed to complete request before timeout, error = %b", error.get()));
    }

    for (ResourceType type : ResourceType.values()) {
      assertThat(configWatcher.counts).containsEntry(type, 1);
    }

    assertThat(configWatcher.counts).hasSize(ResourceType.values().length);

    for (ResourceType type : ResourceType.values()) {
      assertThat(responses).haveAtLeastOne(new Condition<>(
          r -> r.getTypeUrl().equals(type.typeUrl()) && r.getVersionInfo().equals(VERSION),
          "missing expected response of type %s", type));
    }
  }

  private static Multimap<ResourceType, Response> createResponses() {
    return ImmutableMultimap.<ResourceType, Response>builder()
        .put(ResourceType.CLUSTER, Response.create(false, ImmutableList.of(CLUSTER), VERSION))
        .put(ResourceType.ENDPOINT, Response.create(false, ImmutableList.of(ENDPOINT), VERSION))
        .put(ResourceType.LISTENER, Response.create(false, ImmutableList.of(LISTENER), VERSION))
        .put(ResourceType.ROUTE, Response.create(false, ImmutableList.of(ROUTE), VERSION))
        .build();
  }

  private static class MockConfigWatcher implements ConfigWatcher {

    private final boolean closeWatch;
    private final Map<ResourceType, Integer> counts;
    private final LinkedListMultimap<ResourceType, Response> responses;

    MockConfigWatcher(boolean closeWatch, Multimap<ResourceType, Response> responses) {
      this.closeWatch = closeWatch;
      this.counts = new HashMap<>();
      this.responses = LinkedListMultimap.create(responses);
    }

    @Override
    public Watch watch(ResourceType type, Node node, String version, Collection<String> names) {
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
