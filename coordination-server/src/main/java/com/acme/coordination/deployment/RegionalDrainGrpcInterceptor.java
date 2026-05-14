package com.acme.coordination.deployment;

import com.acme.coordination.region.RegionSwitchDrainPolicy;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Set;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@GrpcGlobalServerInterceptor
@Order(Ordered.HIGHEST_PRECEDENCE + 25)
public class RegionalDrainGrpcInterceptor implements ServerInterceptor {

  private static final Set<String> REGIONAL_LEASE_RPC = Set.of(
      "coordination.v1.Coordination/AcquireCoordinator",
      "coordination.v1.Coordination/RenewCoordinator",
      "coordination.v1.Coordination/RegisterWorker",
      "coordination.v1.Coordination/WorkerHeartbeat",
      "coordination.v1.Coordination/AcquireJob",
      "coordination.v1.Coordination/ReleaseJob",
      "coordination.v1.Coordination/RenewJob");

  private final RegionSwitchDrainPolicy drain;

  public RegionalDrainGrpcInterceptor(RegionSwitchDrainPolicy drain) {
    this.drain = drain;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next
  ) {
    String full = call.getMethodDescriptor().getFullMethodName();
    if (!REGIONAL_LEASE_RPC.contains(full) || drain.regionalWritesAllowed()) {
      return next.startCall(call, headers);
    }
    call.close(
        Status.FAILED_PRECONDITION.withDescription(
            "REGION_DRAIN: previous active region still has leader/workers/jobs keys; wait for TTL expiry."),
        new Metadata());
    return new ServerCall.Listener<>() {};
  }
}
