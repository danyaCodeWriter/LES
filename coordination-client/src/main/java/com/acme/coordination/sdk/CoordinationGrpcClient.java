package com.acme.coordination.sdk;

import com.acme.coordination.v1.AcquireCoordinatorRequest;
import com.acme.coordination.v1.AcquireCoordinatorResponse;
import com.acme.coordination.v1.AcquireGlobalJobRequest;
import com.acme.coordination.v1.AcquireJobRequest;
import com.acme.coordination.v1.AcquireJobResponse;
import com.acme.coordination.v1.CoordinationGrpc;
import com.acme.coordination.v1.GetCoordinatorRequest;
import com.acme.coordination.v1.GetCoordinatorResponse;
import com.acme.coordination.v1.GetGlobalCoordinatorRequest;
import com.acme.coordination.v1.GetGlobalJobRequest;
import com.acme.coordination.v1.GetJobRequest;
import com.acme.coordination.v1.GetJobResponse;
import com.acme.coordination.v1.ListGlobalWorkersRequest;
import com.acme.coordination.v1.ListWorkersRequest;
import com.acme.coordination.v1.ListWorkersResponse;
import com.acme.coordination.v1.RegisterGlobalWorkerRequest;
import com.acme.coordination.v1.RegisterWorkerRequest;
import com.acme.coordination.v1.RegisterWorkerResponse;
import com.acme.coordination.v1.ReleaseGlobalJobRequest;
import com.acme.coordination.v1.ReleaseJobRequest;
import com.acme.coordination.v1.ReleaseJobResponse;
import com.acme.coordination.v1.RenewGlobalJobRequest;
import com.acme.coordination.v1.RenewGlobalCoordinatorRequest;
import com.acme.coordination.v1.RenewJobRequest;
import com.acme.coordination.v1.RenewJobResponse;
import com.acme.coordination.v1.RenewCoordinatorRequest;
import com.acme.coordination.v1.RenewCoordinatorResponse;
import com.acme.coordination.v1.StringMap;
import com.acme.coordination.v1.WorkerHeartbeatGlobalRequest;
import com.acme.coordination.v1.WorkerHeartbeatRequest;
import com.acme.coordination.v1.WorkerHeartbeatResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Blocking gRPC facade for common mutations (leases / heartbeats). Prefer async stubs if you run many calls
 * from reactive or high-throughput threads.
 */
public final class CoordinationGrpcClient implements AutoCloseable {

  private final ManagedChannel channel;
  private final CoordinationGrpc.CoordinationBlockingStub stub;

  public CoordinationGrpcClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(Objects.requireNonNull(host), port).usePlaintext().build());
  }

  public CoordinationGrpcClient(ManagedChannel channel) {
    this.channel = Objects.requireNonNull(channel);
    this.stub = CoordinationGrpc.newBlockingStub(channel);
  }

  private static StringMap meta(Map<String, String> metadata) {
    return StringMap.newBuilder().putAllEntries(metadata == null ? Map.of() : metadata).build();
  }

  public AcquireCoordinatorResponse acquireCoordinator(
      String tenantId,
      String coordinatorId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    AcquireCoordinatorRequest.Builder b = AcquireCoordinatorRequest.newBuilder()
        .setTenantId(tenantId)
        .setCoordinatorId(coordinatorId == null ? "" : coordinatorId)
        .setInstanceId(instanceId == null ? "" : instanceId)
        .setTtlSeconds(ttlSeconds)
        .setMetadata(meta(metadata));
    return stub.acquireCoordinator(b.build());
  }

  public RenewCoordinatorResponse renewCoordinator(String tenantId, String token) {
    return stub.renewCoordinator(
        RenewCoordinatorRequest.newBuilder()
            .setTenantId(tenantId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public GetCoordinatorResponse getCoordinator(String tenantId, String coordinatorReadToken) {
    return stub.getCoordinator(
        GetCoordinatorRequest.newBuilder()
            .setTenantId(tenantId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken)
            .build());
  }

  public GetCoordinatorResponse getGlobalCoordinator(String tenantId, String coordinatorReadToken) {
    return stub.getGlobalCoordinator(
        GetGlobalCoordinatorRequest.newBuilder()
            .setTenantId(tenantId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken)
            .build());
  }

  public RenewCoordinatorResponse renewGlobalCoordinator(String tenantId, String token) {
    return stub.renewGlobalCoordinator(
        RenewGlobalCoordinatorRequest.newBuilder()
            .setTenantId(tenantId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public RegisterWorkerResponse registerWorker(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return stub.registerWorker(
        RegisterWorkerRequest.newBuilder()
            .setTenantId(tenantId)
            .setWorkerId(workerId == null ? "" : workerId)
            .setInstanceId(instanceId == null ? "" : instanceId)
            .setTtlSeconds(ttlSeconds)
            .setMetadata(meta(metadata))
            .build());
  }

  public WorkerHeartbeatResponse workerHeartbeat(String tenantId, String workerId, String token) {
    return stub.workerHeartbeat(
        WorkerHeartbeatRequest.newBuilder()
            .setTenantId(tenantId)
            .setWorkerId(workerId == null ? "" : workerId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public ListWorkersResponse listWorkers(String tenantId, String coordinatorReadToken) {
    return stub.listWorkers(
        ListWorkersRequest.newBuilder()
            .setTenantId(tenantId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken)
            .build());
  }

  public ListWorkersResponse listGlobalWorkers(String tenantId, String coordinatorReadToken) {
    return stub.listGlobalWorkers(
        ListGlobalWorkersRequest.newBuilder()
            .setTenantId(tenantId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken)
            .build());
  }

  public RegisterWorkerResponse registerGlobalWorker(
      String tenantId,
      String workerId,
      String instanceId,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return stub.registerGlobalWorker(
        RegisterGlobalWorkerRequest.newBuilder()
            .setTenantId(tenantId)
            .setWorkerId(workerId == null ? "" : workerId)
            .setInstanceId(instanceId == null ? "" : instanceId)
            .setTtlSeconds(ttlSeconds)
            .setMetadata(meta(metadata))
            .build());
  }

  public WorkerHeartbeatResponse workerHeartbeatGlobal(String tenantId, String workerId, String token) {
    return stub.workerHeartbeatGlobal(
        WorkerHeartbeatGlobalRequest.newBuilder()
            .setTenantId(tenantId)
            .setWorkerId(workerId == null ? "" : workerId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public AcquireJobResponse acquireJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return stub.acquireJob(
        AcquireJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setHolder(holder == null ? "" : holder)
            .setParticipantToken(participantToken == null ? "" : participantToken)
            .setTtlSeconds(ttlSeconds)
            .setMetadata(meta(metadata))
            .build());
  }

  public RenewJobResponse renewJob(String tenantId, String jobId, String token) {
    return stub.renewJob(
        RenewJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public ReleaseJobResponse releaseJob(String tenantId, String jobId, String token) {
    return stub.releaseJob(
        ReleaseJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public GetJobResponse getJob(String tenantId, String jobId, String coordinatorReadToken) {
    return getJob(tenantId, jobId, coordinatorReadToken, null);
  }

  public GetJobResponse getJob(
      String tenantId, String jobId, String coordinatorReadToken, String workerRegisterToken
  ) {
    GetJobRequest.Builder b =
        GetJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken);
    if (workerRegisterToken != null && !workerRegisterToken.isBlank()) {
      b.setWorkerRegisterToken(workerRegisterToken);
    }
    return stub.getJob(b.build());
  }

  public AcquireJobResponse acquireGlobalJob(
      String tenantId,
      String jobId,
      String holder,
      String participantToken,
      int ttlSeconds,
      Map<String, String> metadata
  ) {
    return stub.acquireGlobalJob(
        AcquireGlobalJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setHolder(holder == null ? "" : holder)
            .setParticipantToken(participantToken == null ? "" : participantToken)
            .setTtlSeconds(ttlSeconds)
            .setMetadata(meta(metadata))
            .build());
  }

  public RenewJobResponse renewGlobalJob(String tenantId, String jobId, String token) {
    return stub.renewGlobalJob(
        RenewGlobalJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public ReleaseJobResponse releaseGlobalJob(String tenantId, String jobId, String token) {
    return stub.releaseGlobalJob(
        ReleaseGlobalJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setToken(token == null ? "" : token)
            .build());
  }

  public GetJobResponse getGlobalJob(String tenantId, String jobId, String coordinatorReadToken) {
    return getGlobalJob(tenantId, jobId, coordinatorReadToken, null);
  }

  public GetJobResponse getGlobalJob(
      String tenantId, String jobId, String coordinatorReadToken, String workerRegisterToken
  ) {
    GetGlobalJobRequest.Builder b =
        GetGlobalJobRequest.newBuilder()
            .setTenantId(tenantId)
            .setJobId(jobId == null ? "" : jobId)
            .setCoordinatorReadToken(coordinatorReadToken == null ? "" : coordinatorReadToken);
    if (workerRegisterToken != null && !workerRegisterToken.isBlank()) {
      b.setWorkerRegisterToken(workerRegisterToken);
    }
    return stub.getGlobalJob(b.build());
  }

  @Override
  public void close() throws InterruptedException {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }
}
