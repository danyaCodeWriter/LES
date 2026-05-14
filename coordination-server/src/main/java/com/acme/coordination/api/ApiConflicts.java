package com.acme.coordination.api;

import com.acme.coordination.api.error.ResourceConflictException;
import com.acme.coordination.service.CoordinatorService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.WorkerRegistryService;
import java.util.Map;

final class ApiConflicts {

  private ApiConflicts() {}

  static CoordinatorService.AcquireResult requireNewCoordinator(CoordinatorService.AcquireResult r) {
    if (!r.acquired() && r.currentHolder().isPresent()) {
      throw new ResourceConflictException(
          "COORDINATOR_EXISTS",
          "Координатор для этого tenant и active region уже существует.",
          Map.of("currentHolder", r.currentHolder().get()));
    }
    return r;
  }

  static JobLockService.AcquireJobResult requireNewJobLock(JobLockService.AcquireJobResult r) {
    if (!r.acquired() && r.currentHolder().isPresent()) {
      throw new ResourceConflictException(
          "JOB_EXISTS",
          "Блокировка для этого jobId уже существует.",
          Map.of("currentHolder", r.currentHolder().get()));
    }
    return r;
  }

  static WorkerRegistryService.RegisterResult requireNewWorker(WorkerRegistryService.RegisterResult r) {
    if (!r.created() && r.alreadyRegistered().isPresent()) {
      throw new ResourceConflictException(
          "WORKER_EXISTS",
          "Воркер с таким workerId уже зарегистрирован.",
          Map.of("alreadyRegistered", r.alreadyRegistered().get()));
    }
    return r;
  }
}
