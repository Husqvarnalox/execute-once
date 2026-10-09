# 0005. Redis is a cache and a limiter only; every Redis failure fails open

Status: Accepted

## Context

Redis is used for the replay cache in the gateway and for fixed-window rate limiting in the relay and consumer. It is
not part of the durability chain. An unavailable Redis must not stop traffic or change correctness.

## Decision

- Every Redis read or write is wrapped so that an error is logged and the code path continues as if Redis had no
  answer: the gateway goes to Postgres, the limiters allow the request.
- Clients are configured to fail fast (short connect timeout, no offline command queue, one retry) so that a Redis
  outage costs milliseconds per request, not a hung connection.
- Rate limiting can be disabled per service by setting `rate-limit-per-second` to 0, which skips Redis entirely.

## Consequences

- Correctness never depends on Redis; the integration tests run the gateway with Redis stopped.
- During an outage the rate limiters do not limit. That is the intended trade-off for a backpressure mechanism that
  is advisory rather than protective. A protective limit would have to live in the database or in the broker.
- The fixed-window limiter allows up to 2x the configured rate at a window boundary. It is a simple global knob, not a
  fairness mechanism.
