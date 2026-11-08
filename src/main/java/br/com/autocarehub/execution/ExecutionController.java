package br.com.autocarehub.execution;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class ExecutionController {
    private final ExecutionQueue jobs;
    private final byte[] internalToken;

    ExecutionController(ExecutionQueue jobs, @Value("${EXECUTION_INTERNAL_TOKEN:}") String token) {
        this.jobs = jobs;
        this.internalToken = token.getBytes(StandardCharsets.UTF_8);
    }

    @GetMapping("/internal/v1/branches/{branchId}/execution")
    List<JobView> list(@PathVariable UUID branchId, @RequestHeader("X-Internal-Service-Token") String token) {
        authorize(token);
        return jobs.active(branchId).stream().map(JobView::from).toList();
    }

    @PatchMapping("/internal/v1/branches/{branchId}/execution/{orderId}")
    JobView advance(
            @PathVariable UUID branchId,
            @PathVariable UUID orderId,
            @RequestHeader("X-Internal-Service-Token") String token,
            @RequestBody UpdateStatus update) {
        authorize(token);
        if (update.status() == null
                || update.status() == ExecutionJob.Status.REPAIR_RELEASED
                || update.status() == ExecutionJob.Status.RELEASE_FAILED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid operational transition");
        }
        return JobView.from(jobs.advance(branchId, orderId, update.status()));
    }

    private void authorize(String token) {
        if (internalToken.length == 0
                || !MessageDigest.isEqual(internalToken, token.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
    }

    record UpdateStatus(ExecutionJob.Status status) {}

    record JobView(String orderId, String branchId, ExecutionJob.Status status, Instant createdAt) {
        static JobView from(ExecutionJob job) {
            return new JobView(job.orderId(), job.branchId(), job.status(), job.createdAt());
        }
    }
}
