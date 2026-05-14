package com.acme.coordination.api.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ResourceConflictExceptionHandler {

  @ExceptionHandler(ResourceConflictException.class)
  public ResponseEntity<Object> handleConflict(ResourceConflictException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.responseBody());
  }

  @ExceptionHandler(ParticipantException.class)
  public ResponseEntity<Object> handleParticipant(ParticipantException ex) {
    return ResponseEntity.status(ex.getStatus()).body(ex.responseBody());
  }
}
