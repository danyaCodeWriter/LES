package com.acme.coordination.service;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.model.CoordinatorRecord;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Job acquire: {@code holder} must be the active regional/global coordinator id with matching coordinator token, or a
 * registered worker id with matching worker registration token. The token is required so knowledge of an id alone is
 * not sufficient to take locks.
 */
@Service
public class JobParticipantVerificationService {

  private final CoordinatorService coordinators;
  private final WorkerRegistryService workers;

  public JobParticipantVerificationService(CoordinatorService coordinators, WorkerRegistryService workers) {
    this.coordinators = coordinators;
    this.workers = workers;
  }

  public CompletableFuture<Void> verifyRegional(
      String tenantId, String regionId, String holder, String participantToken
  ) {
    requireToken(participantToken);
    requireHolder(holder);
    return coordinators
        .get(tenantId, regionId)
        .thenCompose(
            coordOpt -> {
              if (coordOpt.isPresent()) {
                CoordinatorRecord c = coordOpt.get().record();
                if (holder.equals(c.coordinatorId())) {
                  if (c.token() != null && !c.token().isBlank() && participantToken.equals(c.token())) {
                    return CompletableFuture.completedFuture(null);
                  }
                  return CompletableFuture.failedFuture(
                      new ParticipantException(
                          HttpStatus.FORBIDDEN,
                          "JOB_PARTICIPANT_INVALID",
                          "holder matches coordinatorId but participantToken does not match the active coordinator token."));
                }
              }
              return workers
                  .getWorker(tenantId, regionId, holder)
                  .thenAccept(
                      wOpt -> {
                        if (wOpt.isEmpty() || !participantToken.equals(wOpt.get().token())) {
                          throw new ParticipantException(
                              HttpStatus.FORBIDDEN,
                              "JOB_PARTICIPANT_INVALID",
                              "holder must be the active regional coordinatorId with matching coordinator token, "
                                  + "or a registered workerId in this region with matching register token.");
                        }
                      });
            });
  }

  public CompletableFuture<Void> verifyGlobal(String tenantId, String holder, String participantToken) {
    requireToken(participantToken);
    requireHolder(holder);
    return coordinators
        .getGlobal(tenantId)
        .thenCompose(
            coordOpt -> {
              if (coordOpt.isPresent()) {
                CoordinatorRecord c = coordOpt.get().record();
                if (holder.equals(c.coordinatorId())) {
                  if (c.token() != null && !c.token().isBlank() && participantToken.equals(c.token())) {
                    return CompletableFuture.completedFuture(null);
                  }
                  return CompletableFuture.failedFuture(
                      new ParticipantException(
                          HttpStatus.FORBIDDEN,
                          "JOB_PARTICIPANT_INVALID",
                          "holder matches global coordinatorId but participantToken does not match the active coordinator token."));
                }
              }
              return workers
                  .getWorkerGlobal(tenantId, holder)
                  .thenAccept(
                      wOpt -> {
                        if (wOpt.isEmpty() || !participantToken.equals(wOpt.get().token())) {
                          throw new ParticipantException(
                              HttpStatus.FORBIDDEN,
                              "JOB_PARTICIPANT_INVALID",
                              "holder must be the active global coordinatorId with matching coordinator token, "
                                  + "or a registered global workerId with matching register token.");
                        }
                      });
            });
  }

  private static void requireHolder(String holder) {
    if (holder == null || holder.isBlank()) {
      throw new ParticipantException(
          HttpStatus.BAD_REQUEST,
          "JOB_HOLDER_REQUIRED",
          "holder must be non-blank (regional/global coordinatorId or workerId).");
    }
  }

  private static void requireToken(String participantToken) {
    if (participantToken == null || participantToken.isBlank()) {
      throw new ParticipantException(
          HttpStatus.BAD_REQUEST,
          "JOB_PARTICIPANT_TOKEN_REQUIRED",
          "participantToken is required: coordinator token from acquire, or worker token from register.");
    }
  }
}
