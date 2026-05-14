package com.acme.coordination.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

/**
 * @param virtualNodes optional; if null or not positive, the server uses default (128), same as gRPC when unset.
 * @param withPos if true, response includes {@code points} (vnode positions); omitted or false — only {@code ranges}.
 */
public record ComputeRangesRequest(
    @NotEmpty List<String> workerIds,
    Integer virtualNodes,
    @JsonAlias({"withpos"})
    Boolean withPos) {}

