package com.acme.coordination.grpc;

import com.acme.coordination.hash.ConsistentHashService;
import com.acme.coordination.ring.ConsistentHashRing;
import com.acme.coordination.model.CoordinatorRecord;
import com.acme.coordination.region.RegionalScope;
import com.acme.coordination.model.JobRecord;
import com.acme.coordination.model.WorkerRecord;
import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.etcd.Keyspace;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import com.acme.coordination.service.CoordinatorService;
import com.acme.coordination.service.JobLockReadVerificationService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.JobParticipantVerificationService;
import com.acme.coordination.security.SreTokenVerifier;
import com.acme.coordination.service.DrainService;
import com.acme.coordination.service.WorkerRegistryService;
import com.acme.coordination.v1.AcquireCoordinatorRequest;
import com.acme.coordination.v1.AcquireCoordinatorResponse;
import com.acme.coordination.v1.AcquireGlobalCoordinatorRequest;
import com.acme.coordination.v1.AcquireGlobalJobRequest;
import com.acme.coordination.v1.AcquireJobRequest;
import com.acme.coordination.v1.AcquireJobResponse;
import com.acme.coordination.v1.ComputeRangesRequest;
import com.acme.coordination.v1.ComputeRangesResponse;
import com.acme.coordination.v1.CoordinationGrpc;
import com.acme.coordination.v1.CoordinatorPayload;
import com.acme.coordination.v1.GetCoordinatorRequest;
import com.acme.coordination.v1.GetCoordinatorResponse;
import com.acme.coordination.v1.GetGlobalCoordinatorRequest;
import com.acme.coordination.v1.GetGlobalJobRequest;
import com.acme.coordination.v1.GetJobRequest;
import com.acme.coordination.v1.GetJobResponse;
import com.acme.coordination.v1.JobPayload;
import com.acme.coordination.v1.ListGlobalWorkersRequest;
import com.acme.coordination.v1.ListWorkersRequest;
import com.acme.coordination.v1.ListWorkersResponse;
import com.acme.coordination.v1.Range;
import com.acme.coordination.v1.RegisterGlobalWorkerRequest;
import com.acme.coordination.v1.RegisterWorkerRequest;
import com.acme.coordination.v1.RegisterWorkerResponse;
import com.acme.coordination.v1.ReleaseGlobalJobRequest;
import com.acme.coordination.v1.ReleaseJobRequest;
import com.acme.coordination.v1.ReleaseJobResponse;
import com.acme.coordination.v1.RenewGlobalJobRequest;
import com.acme.coordination.v1.RenewJobRequest;
import com.acme.coordination.v1.RenewJobResponse;
import com.acme.coordination.v1.RenewCoordinatorRequest;
import com.acme.coordination.v1.RenewCoordinatorResponse;
import com.acme.coordination.v1.RenewGlobalCoordinatorRequest;
import com.acme.coordination.v1.RingPoint;
import com.acme.coordination.v1.Ranges;
import com.acme.coordination.v1.StringMap;
import com.acme.coordination.v1.TenantKeyWatchEvent;
import com.acme.coordination.v1.WatchTenantKeysRequest;
import com.acme.coordination.v1.WorkerHeartbeatGlobalRequest;
import com.acme.coordination.v1.WorkerHeartbeatRequest;
import com.acme.coordination.v1.WorkerHeartbeatResponse;
import com.acme.coordination.v1.WorkerPayload;
import com.acme.coordination.watch.EtcdWatchService;
import com.acme.coordination.watch.WatchEventDto;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.http.HttpStatus;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

@GrpcService
public class CoordinationGrpcService extends CoordinationGrpc.CoordinationImplBase {

  private final CoordinatorService coordinator;
  private final WorkerRegistryService workers;
  private final JobLockService jobs;
  private final JobParticipantVerificationService jobParticipants;
  private final CoordinatorReadVerificationService readAuth;
  private final JobLockReadVerificationService jobReadAuth;
  private final ConsistentHashService ring;
  private final RegionalScope regionalScope;
  private final EtcdWatchService watch;
  private final Keyspace keyspace;
  private final DrainService drain;
  private final SreTokenVerifier sre;

  public CoordinationGrpcService(
      CoordinatorService coordinator,
      WorkerRegistryService workers,
      JobLockService jobs,
      JobParticipantVerificationService jobParticipants,
      CoordinatorReadVerificationService readAuth,
      JobLockReadVerificationService jobReadAuth,
      ConsistentHashService ring,
      RegionalScope regionalScope,
      EtcdWatchService watch,
      Keyspace keyspace,
      DrainService drain,
      SreTokenVerifier sre
  ) {
    this.coordinator = coordinator;
    this.workers = workers;
    this.jobs = jobs;
    this.jobParticipants = jobParticipants;
    this.readAuth = readAuth;
    this.jobReadAuth = jobReadAuth;
    this.ring = ring;
    this.regionalScope = regionalScope;
    this.watch = watch;
    this.keyspace = keyspace;
    this.drain = drain;
    this.sre = sre;
  }

  @Override
  public void acquireCoordinator(AcquireCoordinatorRequest request, StreamObserver<AcquireCoordinatorResponse> responseObserver) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    coordinator.acquire(
            request.getTenantId(),
            regionalScope.regionId(),
            request.getCoordinatorId(),
            request.getInstanceId(),
            request.getTtlSeconds(),
            md)
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.acquired() && res.currentHolder().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("COORDINATOR_EXISTS: Координатор уже занят.").asRuntimeException());
            return;
          }
          AcquireCoordinatorResponse.Builder b = AcquireCoordinatorResponse.newBuilder()
              .setAcquired(res.acquired())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId())
              .setRevision(res.revision() == null ? 0 : res.revision())
              .setFenceEpoch(res.fenceEpoch() == null ? 0 : res.fenceEpoch());
          if (res.acquired() && res.token() != null) {
            b.setToken(res.token());
          }
          res.currentHolder().ifPresent(holder -> b.setCurrentHolder(toPayload(holder, 0, 0)));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void getCoordinator(GetCoordinatorRequest request, StreamObserver<GetCoordinatorResponse> responseObserver) {
    readAuth
        .verifyRegionalGrpc(request.getTenantId(), CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()))
        .thenCompose(
            v -> coordinator.get(request.getTenantId(), regionalScope.regionId()))
        .whenComplete((opt, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          GetCoordinatorResponse.Builder b = GetCoordinatorResponse.newBuilder();
          opt.ifPresent(st -> b.setCoordinator(toPayloadRead(st.record(), st.leaseId(), st.modRevision())));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void renewCoordinator(RenewCoordinatorRequest request, StreamObserver<RenewCoordinatorResponse> responseObserver) {
    coordinator.renewIfHolder(request.getTenantId(), regionalScope.regionId(), request.getToken())
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(RenewCoordinatorResponse.newBuilder().setRenewed(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void releaseCoordinator(
      com.acme.coordination.v1.ReleaseCoordinatorRequest request,
      StreamObserver<com.acme.coordination.v1.ReleaseCoordinatorResponse> responseObserver) {
    coordinator
        .releaseIfHolder(request.getTenantId(), regionalScope.regionId(), request.getToken())
        .whenComplete(
            (ok, err) -> {
              if (err != null) {
                responseObserver.onError(toGrpcError(err));
                return;
              }
              responseObserver.onNext(
                  com.acme.coordination.v1.ReleaseCoordinatorResponse.newBuilder().setReleased(ok).build());
              responseObserver.onCompleted();
            });
  }

  @Override
  public void acquireGlobalCoordinator(
      AcquireGlobalCoordinatorRequest request,
      StreamObserver<AcquireCoordinatorResponse> responseObserver
  ) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    coordinator.acquireGlobal(
            request.getTenantId(),
            request.getCoordinatorId(),
            request.getInstanceId(),
            request.getTtlSeconds(),
            md)
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.acquired() && res.currentHolder().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("COORDINATOR_EXISTS: Координатор уже занят.").asRuntimeException());
            return;
          }
          AcquireCoordinatorResponse.Builder b = AcquireCoordinatorResponse.newBuilder()
              .setAcquired(res.acquired())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId())
              .setRevision(res.revision() == null ? 0 : res.revision())
              .setFenceEpoch(res.fenceEpoch() == null ? 0 : res.fenceEpoch());
          if (res.acquired() && res.token() != null) {
            b.setToken(res.token());
          }
          res.currentHolder().ifPresent(holder -> b.setCurrentHolder(toPayload(holder, 0, 0)));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void getGlobalCoordinator(GetGlobalCoordinatorRequest request, StreamObserver<GetCoordinatorResponse> responseObserver) {
    readAuth
        .verifyGlobalGrpc(request.getTenantId(), CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()))
        .thenCompose(v -> coordinator.getGlobal(request.getTenantId()))
        .whenComplete((opt, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          GetCoordinatorResponse.Builder b = GetCoordinatorResponse.newBuilder();
          opt.ifPresent(st -> b.setCoordinator(toPayloadRead(st.record(), st.leaseId(), st.modRevision())));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void renewGlobalCoordinator(
      RenewGlobalCoordinatorRequest request,
      StreamObserver<RenewCoordinatorResponse> responseObserver
  ) {
    coordinator.renewGlobalIfHolder(request.getTenantId(), request.getToken())
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(RenewCoordinatorResponse.newBuilder().setRenewed(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void releaseGlobalCoordinator(
      com.acme.coordination.v1.ReleaseGlobalCoordinatorRequest request,
      StreamObserver<com.acme.coordination.v1.ReleaseCoordinatorResponse> responseObserver) {
    coordinator
        .releaseGlobalIfHolder(request.getTenantId(), request.getToken())
        .whenComplete(
            (ok, err) -> {
              if (err != null) {
                responseObserver.onError(toGrpcError(err));
                return;
              }
              responseObserver.onNext(
                  com.acme.coordination.v1.ReleaseCoordinatorResponse.newBuilder().setReleased(ok).build());
              responseObserver.onCompleted();
            });
  }

  @Override
  public void drainRegional(
      com.acme.coordination.v1.DrainRegionalRequest request,
      StreamObserver<com.acme.coordination.v1.DrainResponse> responseObserver) {
    try {
      sre.assertSreFromGrpc(request.getAccessCredential());
    } catch (RuntimeException e) {
      responseObserver.onError(toGrpcError(e));
      return;
    }
    drain
        .drainRegional(request.getTenantId(), request.getRegionId())
        .whenComplete((rep, err) -> completeDrain(responseObserver, rep, err));
  }

  @Override
  public void drainGlobal(
      com.acme.coordination.v1.DrainGlobalRequest request,
      StreamObserver<com.acme.coordination.v1.DrainResponse> responseObserver) {
    try {
      sre.assertSreFromGrpc(request.getAccessCredential());
    } catch (RuntimeException e) {
      responseObserver.onError(toGrpcError(e));
      return;
    }
    drain
        .drainGlobalLeases(request.getTenantId())
        .whenComplete((rep, err) -> completeDrain(responseObserver, rep, err));
  }

  private void completeDrain(
      StreamObserver<com.acme.coordination.v1.DrainResponse> responseObserver,
      DrainService.DrainReport rep,
      Throwable err) {
    if (err != null) {
      responseObserver.onError(toGrpcError(err));
      return;
    }
    responseObserver.onNext(
        com.acme.coordination.v1.DrainResponse.newBuilder()
            .setTenantId(rep.tenantId())
            .setScope(rep.scope())
            .setDeletedCount(rep.deletedCount())
            .addAllDeletedKeys(rep.deletedKeys())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    workers.register(
            request.getTenantId(),
            regionalScope.regionId(),
            request.getWorkerId(),
            request.getInstanceId(),
            request.getTtlSeconds(),
            md)
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.created() && res.alreadyRegistered().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("WORKER_EXISTS: Воркер с таким workerId уже зарегистрирован.").asRuntimeException());
            return;
          }
          RegisterWorkerResponse.Builder b = RegisterWorkerResponse.newBuilder()
              .setCreated(res.created())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId());
          if (res.token() != null) {
            b.setToken(res.token());
          }
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void workerHeartbeat(WorkerHeartbeatRequest request, StreamObserver<WorkerHeartbeatResponse> responseObserver) {
    workers.heartbeat(request.getTenantId(), regionalScope.regionId(), request.getWorkerId(), request.getToken())
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(WorkerHeartbeatResponse.newBuilder().setOk(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void listWorkers(ListWorkersRequest request, StreamObserver<ListWorkersResponse> responseObserver) {
    readAuth
        .verifyRegionalGrpc(request.getTenantId(), CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()))
        .thenCompose(v -> workers.listWorkers(request.getTenantId(), regionalScope.regionId()))
        .whenComplete((list, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          ListWorkersResponse.Builder b = ListWorkersResponse.newBuilder();
          for (WorkerRegistryService.WorkerState st : list) {
            b.addWorkers(toWorkerPayloadRead(st.record(), st.leaseId(), st.modRevision()));
          }
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void registerGlobalWorker(RegisterGlobalWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    workers.registerGlobal(
            request.getTenantId(),
            request.getWorkerId(),
            request.getInstanceId(),
            request.getTtlSeconds(),
            md)
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.created() && res.alreadyRegistered().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("WORKER_EXISTS: Воркер с таким workerId уже зарегистрирован.").asRuntimeException());
            return;
          }
          RegisterWorkerResponse.Builder b = RegisterWorkerResponse.newBuilder()
              .setCreated(res.created())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId());
          if (res.token() != null) {
            b.setToken(res.token());
          }
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void workerHeartbeatGlobal(WorkerHeartbeatGlobalRequest request, StreamObserver<WorkerHeartbeatResponse> responseObserver) {
    workers.heartbeatGlobal(request.getTenantId(), request.getWorkerId(), request.getToken())
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(WorkerHeartbeatResponse.newBuilder().setOk(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void listGlobalWorkers(ListGlobalWorkersRequest request, StreamObserver<ListWorkersResponse> responseObserver) {
    readAuth
        .verifyGlobalGrpc(request.getTenantId(), CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()))
        .thenCompose(v -> workers.listWorkersGlobal(request.getTenantId()))
        .whenComplete((list, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          ListWorkersResponse.Builder b = ListWorkersResponse.newBuilder();
          for (WorkerRegistryService.WorkerState st : list) {
            b.addWorkers(toWorkerPayloadRead(st.record(), st.leaseId(), st.modRevision()));
          }
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void acquireJob(AcquireJobRequest request, StreamObserver<AcquireJobResponse> responseObserver) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    String participant = request.getParticipantToken();
    CompletableFuture<Void> gate =
        participant == null || participant.isBlank()
            ? CompletableFuture.completedFuture(null)
            : jobParticipants.verifyRegional(
                request.getTenantId(), regionalScope.regionId(), request.getHolder(), participant);
    gate
        .thenCompose(
            v ->
                jobs.acquire(
                    request.getTenantId(),
                    regionalScope.regionId(),
                    request.getJobId(),
                    request.getHolder(),
                    request.getTtlSeconds(),
                    md))
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.acquired() && res.currentHolder().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("JOB_EXISTS: Блокировка для этого jobId уже существует.").asRuntimeException());
            return;
          }
          AcquireJobResponse.Builder b = AcquireJobResponse.newBuilder()
              .setAcquired(res.acquired())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId())
              .setRevision(res.revision() == null ? 0 : res.revision())
              .setToken(res.token() == null ? "" : res.token());
          res.currentHolder().ifPresent(holder -> b.setCurrentHolder(toJobPayload(holder, 0, 0)));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void releaseJob(ReleaseJobRequest request, StreamObserver<ReleaseJobResponse> responseObserver) {
    jobs.releaseIfCredential(
            request.getTenantId(),
            regionalScope.regionId(),
            request.getJobId(),
            grpcJobToken(request.getToken()),
            grpcJobToken(request.getHolder()))
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(ReleaseJobResponse.newBuilder().setReleased(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void renewJob(RenewJobRequest request, StreamObserver<RenewJobResponse> responseObserver) {
    jobs.renewIfCredential(
            request.getTenantId(),
            regionalScope.regionId(),
            request.getJobId(),
            grpcJobToken(request.getToken()),
            grpcJobToken(request.getHolder()))
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(RenewJobResponse.newBuilder().setRenewed(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    jobReadAuth
        .readRegionalJobGrpc(
            request.getTenantId(),
            request.getJobId(),
            CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()),
            request.getWorkerRegisterToken())
        .whenComplete((opt, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          GetJobResponse.Builder b = GetJobResponse.newBuilder();
          opt.ifPresent(st -> b.setJob(toJobPayloadRead(st.record(), st.leaseId(), st.modRevision())));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void acquireGlobalJob(AcquireGlobalJobRequest request, StreamObserver<AcquireJobResponse> responseObserver) {
    Map<String, String> md = request.getMetadata().getEntriesMap();
    String participant = request.getParticipantToken();
    CompletableFuture<Void> gate =
        participant == null || participant.isBlank()
            ? CompletableFuture.completedFuture(null)
            : jobParticipants.verifyGlobal(request.getTenantId(), request.getHolder(), participant);
    gate
        .thenCompose(
            v -> jobs.acquireGlobal(request.getTenantId(), request.getJobId(), request.getHolder(), request.getTtlSeconds(), md))
        .whenComplete((res, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          if (!res.acquired() && res.currentHolder().isPresent()) {
            responseObserver.onError(
                Status.ALREADY_EXISTS.withDescription("JOB_EXISTS: Блокировка для этого jobId уже существует.").asRuntimeException());
            return;
          }
          AcquireJobResponse.Builder b = AcquireJobResponse.newBuilder()
              .setAcquired(res.acquired())
              .setLeaseId(res.leaseId() == null ? 0 : res.leaseId())
              .setRevision(res.revision() == null ? 0 : res.revision())
              .setToken(res.token() == null ? "" : res.token());
          res.currentHolder().ifPresent(holder -> b.setCurrentHolder(toJobPayload(holder, 0, 0)));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void releaseGlobalJob(ReleaseGlobalJobRequest request, StreamObserver<ReleaseJobResponse> responseObserver) {
    jobs.releaseGlobalIfCredential(
            request.getTenantId(),
            request.getJobId(),
            grpcJobToken(request.getToken()),
            grpcJobToken(request.getHolder()))
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(ReleaseJobResponse.newBuilder().setReleased(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void renewGlobalJob(RenewGlobalJobRequest request, StreamObserver<RenewJobResponse> responseObserver) {
    jobs.renewGlobalIfCredential(
            request.getTenantId(),
            request.getJobId(),
            grpcJobToken(request.getToken()),
            grpcJobToken(request.getHolder()))
        .whenComplete((ok, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          responseObserver.onNext(RenewJobResponse.newBuilder().setRenewed(ok).build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void getGlobalJob(GetGlobalJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    jobReadAuth
        .readGlobalJobGrpc(
            request.getTenantId(),
            request.getJobId(),
            CoordinatorReadVerificationService.grpcReadSecret(request.getCoordinatorReadToken()),
            request.getWorkerRegisterToken())
        .whenComplete((opt, err) -> {
          if (err != null) {
            responseObserver.onError(toGrpcError(err));
            return;
          }
          GetJobResponse.Builder b = GetJobResponse.newBuilder();
          opt.ifPresent(st -> b.setJob(toJobPayloadRead(st.record(), st.leaseId(), st.modRevision())));
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
        });
  }

  @Override
  public void watchTenantKeys(WatchTenantKeysRequest request, StreamObserver<TenantKeyWatchEvent> responseObserver) {
    String tenantId = request.getTenantId();
    readAuth
        .verifyTenantKeysStreamGrpc(
            tenantId, CoordinatorReadVerificationService.grpcReadSecret(request.getAccessCredential()))
        .whenComplete(
            (ok, err) -> {
              if (err != null) {
                responseObserver.onError(toGrpcError(err));
                return;
              }
              Flux<WatchEventDto> flux =
                  Flux.merge(
                      watch.watchPrefix(keyspace.regionsPrefix(tenantId)),
                      watch.watchPrefix(keyspace.globalSubtreePrefix(tenantId)));
              Disposable d =
                  flux.subscribe(
                      ev -> responseObserver.onNext(toTenantKeyWatchEvent(ev)),
                      e -> responseObserver.onError(toGrpcError(e)),
                      responseObserver::onCompleted);
              if (responseObserver instanceof ServerCallStreamObserver<TenantKeyWatchEvent> obs) {
                obs.setOnCancelHandler(d::dispose);
              }
            });
  }

  private static TenantKeyWatchEvent toTenantKeyWatchEvent(WatchEventDto ev) {
    return TenantKeyWatchEvent.newBuilder()
        .setEventType(ev.type())
        .setKey(ev.key())
        .setModRevision(ev.modRevision())
        .setLeaseId(ev.leaseId())
        .setValueJson(ev.valueJson())
        .build();
  }

  @Override
  public void computeRanges(ComputeRangesRequest request, StreamObserver<ComputeRangesResponse> responseObserver) {
    try {
      List<String> ids = request.getWorkerIdsList();
      var res = ring.computeRanges(ids, request.getVirtualNodes());
      ComputeRangesResponse.Builder b = ComputeRangesResponse.newBuilder();
      res.ranges().forEach((workerId, ranges) -> {
        Ranges.Builder rb = Ranges.newBuilder();
        for (ConsistentHashRing.Range r : ranges) {
          rb.addRanges(Range.newBuilder().setStart(r.start()).setEnd(r.end()).build());
        }
        b.putRangesByWorker(workerId, rb.build());
      });
      if (request.getWithPoints()) {
        for (ConsistentHashRing.Point p : res.points()) {
          b.addPoints(
              RingPoint.newBuilder().setPos(p.pos()).setWorkerId(p.workerId()).build());
        }
      }
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  private static RuntimeException toGrpcError(Throwable err) {
    Throwable root = err;
    if (err instanceof CompletionException && err.getCause() != null) {
      root = err.getCause();
    }
    if (root instanceof ParticipantException pe) {
      Status.Code code;
      if (pe.getStatus() == HttpStatus.UNAUTHORIZED) {
        code = Status.Code.UNAUTHENTICATED;
      } else if (pe.getStatus() == HttpStatus.FORBIDDEN) {
        code = Status.Code.PERMISSION_DENIED;
      } else if (pe.getStatus() == HttpStatus.TOO_MANY_REQUESTS) {
        code = Status.Code.RESOURCE_EXHAUSTED;
      } else {
        code = Status.Code.INVALID_ARGUMENT;
      }
      return Status.fromCode(code)
          .withDescription(pe.getCode() + ": " + pe.getMessage())
          .asRuntimeException();
    }
    if (root instanceof IllegalArgumentException iae) {
      return Status.INVALID_ARGUMENT.withDescription(iae.getMessage()).asRuntimeException();
    }
    return Status.INTERNAL.withDescription(err.getMessage()).withCause(err).asRuntimeException();
  }

  private static StringMap toStringMap(Map<String, String> md) {
    if (md == null || md.isEmpty()) {
      return StringMap.getDefaultInstance();
    }
    return StringMap.newBuilder().putAllEntries(md).build();
  }

  private static CoordinatorPayload toPayload(CoordinatorRecord r, long leaseId, long modRevision) {
    CoordinatorPayload.Builder b =
        CoordinatorPayload.newBuilder()
            .setCoordinatorId(r.coordinatorId() == null ? "" : r.coordinatorId())
            .setInstanceId(r.instanceId())
            .setMetadata(toStringMap(r.metadata() == null ? Map.of() : r.metadata()))
            .setIssuedAtUnixMillis(r.issuedAt().toEpochMilli())
            .setFenceEpoch(r.fenceEpoch())
            .setLeaseId(leaseId)
            .setModRevision(modRevision);
    if (r.token() != null) {
      b.setToken(r.token());
    }
    return b.build();
  }

  private static CoordinatorPayload toPayloadRead(CoordinatorRecord r, long leaseId, long modRevision) {
    return toPayload(r, leaseId, modRevision).toBuilder().clearToken().build();
  }

  private static WorkerPayload toWorkerPayload(WorkerRecord r, long leaseId, long modRevision) {
    return WorkerPayload.newBuilder()
        .setWorkerId(r.workerId())
        .setInstanceId(r.instanceId())
        .setToken(r.token())
        .setMetadata(toStringMap(r.metadata() == null ? Map.of() : r.metadata()))
        .setIssuedAtUnixMillis(r.issuedAt().toEpochMilli())
        .setLeaseId(leaseId)
        .setModRevision(modRevision)
        .build();
  }

  private static WorkerPayload toWorkerPayloadRead(WorkerRecord r, long leaseId, long modRevision) {
    return toWorkerPayload(r, leaseId, modRevision).toBuilder().clearToken().build();
  }

  private static JobPayload toJobPayload(JobRecord r, long leaseId, long modRevision) {
    JobPayload.Builder b =
        JobPayload.newBuilder()
            .setJobId(r.jobId())
            .setHolder(r.holder())
            .setMetadata(toStringMap(r.metadata() == null ? Map.of() : r.metadata()))
            .setIssuedAtUnixMillis(r.issuedAt().toEpochMilli())
            .setLeaseId(leaseId)
            .setModRevision(modRevision);
    if (r.token() != null && !r.token().isBlank()) {
      b.setToken(r.token());
    }
    return b.build();
  }

  private static JobPayload toJobPayloadRead(JobRecord r, long leaseId, long modRevision) {
    return toJobPayload(r, leaseId, modRevision).toBuilder().clearToken().build();
  }

  /** Non-empty gRPC string → value; empty → {@code null} (REST-style optional). */
  private static String grpcJobToken(String s) {
    return s == null || s.isEmpty() ? null : s;
  }
}
