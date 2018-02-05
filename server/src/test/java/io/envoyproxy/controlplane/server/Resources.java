package io.envoyproxy.controlplane.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import envoy.api.v2.Cds.Cluster;
import envoy.api.v2.Cds.Cluster.DiscoveryType;
import envoy.api.v2.Cds.Cluster.EdsClusterConfig;
import envoy.api.v2.Eds.ClusterLoadAssignment;
import envoy.api.v2.Lds.Listener;
import envoy.api.v2.Rds.RouteConfiguration;
import envoy.api.v2.core.AddressOuterClass.Address;
import envoy.api.v2.core.AddressOuterClass.SocketAddress;
import envoy.api.v2.core.AddressOuterClass.SocketAddress.Protocol;
import envoy.api.v2.core.ConfigSourceOuterClass.AggregatedConfigSource;
import envoy.api.v2.core.ConfigSourceOuterClass.ApiConfigSource;
import envoy.api.v2.core.ConfigSourceOuterClass.ApiConfigSource.ApiType;
import envoy.api.v2.core.ConfigSourceOuterClass.ConfigSource;
import envoy.api.v2.endpoint.EndpointOuterClass.Endpoint;
import envoy.api.v2.endpoint.EndpointOuterClass.LbEndpoint;
import envoy.api.v2.endpoint.EndpointOuterClass.LocalityLbEndpoints;
import envoy.api.v2.listener.Listener.Filter;
import envoy.api.v2.listener.Listener.FilterChain;
import envoy.api.v2.route.RouteOuterClass.Route;
import envoy.api.v2.route.RouteOuterClass.RouteAction;
import envoy.api.v2.route.RouteOuterClass.RouteMatch;
import envoy.api.v2.route.RouteOuterClass.VirtualHost;
import envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManagerOuterClass.HttpConnectionManager;
import envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManagerOuterClass.HttpConnectionManager.CodecType;
import envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManagerOuterClass.HttpFilter;
import envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManagerOuterClass.Rds;
import java.io.IOException;
import java.net.ServerSocket;

public class Resources {

  private static final String LOCALHOST   = "127.0.0.1";
  private static final String HTTP_FILTER = "envoy.http_connection_manager";
  private static final String ROUTER      = "envoy.router";
  private static final String XDS_CLUSTER = "xds_cluster";

  /**
   * Returns a new test cluster.
   *
   * @param ads use ADS for disco?
   * @param clusterName name of the new cluster
   */
  public static Cluster createCluster(boolean ads, String clusterName) {
    ConfigSource edsSource = ads
        ? ConfigSource.newBuilder()
            .setAds(AggregatedConfigSource.getDefaultInstance())
            .build()
        : ConfigSource.newBuilder()
            .setApiConfigSource(ApiConfigSource.newBuilder()
                .setApiType(ApiType.GRPC)
                .addClusterNames(XDS_CLUSTER)
                .build())
            .build();

    return Cluster.newBuilder()
        .setName(clusterName)
        .setConnectTimeout(Durations.fromSeconds(5))
        .setEdsClusterConfig(EdsClusterConfig.newBuilder()
            .setEdsConfig(edsSource)
            .setServiceName(clusterName)
            .build())
        .setType(DiscoveryType.EDS)
        .build();
  }

  /**
   * Returns a new test endpoint for the given cluster.
   *
   * @param clusterName name of the test cluster that is associated with this endpoint
   * @param port port to use for the endpoint
   */
  public static ClusterLoadAssignment createEndpoint(String clusterName, int port) {
    return ClusterLoadAssignment.newBuilder()
        .setClusterName(clusterName)
        .addEndpoints(LocalityLbEndpoints.newBuilder()
            .addLbEndpoints(LbEndpoint.newBuilder()
                .setEndpoint(Endpoint.newBuilder()
                    .setAddress(Address.newBuilder()
                        .setSocketAddress(SocketAddress.newBuilder()
                            .setAddress(LOCALHOST)
                            .setPortValue(port)
                            .setProtocol(Protocol.TCP)
                            .build())
                        .build())
                    .build())
                .build())
            .build())
        .build();
  }

  /**
   * Returns a new test listener.
   *
   * @param ads use ADS for disco?
   * @param listenerName name of the new listener
   * @param port port to use for the listener
   * @param routeName name of the test route that is associated with this listener
   */
  public static Listener createListener(boolean ads, String listenerName, int port, String routeName) {
    ConfigSource rdsSource = ads
        ? ConfigSource.newBuilder()
            .setAds(AggregatedConfigSource.getDefaultInstance())
            .build()
        : ConfigSource.newBuilder()
            .setApiConfigSource(ApiConfigSource.newBuilder()
                .setApiType(ApiType.GRPC)
                .addClusterNames(XDS_CLUSTER)
                .build())
            .build();

    HttpConnectionManager manager = HttpConnectionManager.newBuilder()
        .setCodecType(CodecType.AUTO)
        .setStatPrefix("http")
        .setRds(Rds.newBuilder()
            .setConfigSource(rdsSource)
            .setRouteConfigName(routeName)
            .build())
        .addHttpFilters(HttpFilter.newBuilder()
            .setName(ROUTER)
            .build())
        .build();

    return Listener.newBuilder()
        .setName(listenerName)
        .setAddress(Address.newBuilder()
            .setSocketAddress(SocketAddress.newBuilder()
                .setAddress(LOCALHOST)
                .setPortValue(port)
                .setProtocol(Protocol.TCP)
                .build())
            .build())
        .addFilterChains(FilterChain.newBuilder()
            .addFilters(Filter.newBuilder()
                .setName(HTTP_FILTER)
                .setConfig(messageAsStruct(manager))
                .build())
            .build())
        .build();
  }

  /**
   * Returns a new test route.
   *
   * @param routeName name of the new route
   * @param clusterName name of the test cluster that is associated with this route
   */
  public static RouteConfiguration createRoute(String routeName, String clusterName) {
    return RouteConfiguration.newBuilder()
        .setName(routeName)
        .addVirtualHosts(VirtualHost.newBuilder()
            .setName("all")
            .addDomains("*")
            .addRoutes(Route.newBuilder()
                .setMatch(RouteMatch.newBuilder()
                    .setPrefix("/")
                    .build())
                .setRoute(RouteAction.newBuilder()
                    .setCluster(clusterName)
                    .build())
                .build())
            .build())
        .build();
  }

  /**
   * Returns a random available port.
   */
  public static int getAvailablePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get an available port", e);
    }
  }

  private static Struct messageAsStruct(MessageOrBuilder message) {
    try {
      String json = JsonFormat.printer()
          .preservingProtoFieldNames()
          .print(message);

      Struct.Builder structBuilder = Struct.newBuilder();

      JsonFormat.parser().merge(json, structBuilder);

      return structBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to convert protobuf message to struct", e);
    }
  }

  private Resources() { }
}
