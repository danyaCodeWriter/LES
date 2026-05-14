package com.acme.coordination.deployment;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.springframework.stereotype.Component;

@Component
@GrpcGlobalServerInterceptor
public class StandbyRegionalMutationGrpcInterceptor implements ServerInterceptor {

  private final DeploymentRegistrationPolicy policy;

  public StandbyRegionalMutationGrpcInterceptor(DeploymentRegistrationPolicy policy) {
    this.policy = policy;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next
  ) {
    String full = call.getMethodDescriptor().getFullMethodName();
    if (policy.shouldBlockGrpcMutation(full)) {
      String detail =
          policy.isSwitch() ? DeploymentRegistrationPolicy.switchGrpcDetail() : DeploymentRegistrationPolicy.standbyGrpcDetail();
      call.close(Status.FAILED_PRECONDITION.withDescription(detail), new Metadata());
      return new ServerCall.Listener<>() {};
    }
    return next.startCall(call, headers);
  }
}
