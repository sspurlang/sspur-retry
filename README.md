# retry

Retries with exponential backoff and jitter, retry budgets and Retry-After hints, a circuit breaker, and token bucket and sliding window rate limiters for SSPUR.

The runners never call the system clock directly: they sleep and read the time through a small `clock` effect. `run_now` and `run_hinted_now` handle it with the real clock (`sleep_ms`, `now_ms`), and `simulate` and `simulate_hinted` with a virtual clock that only moves when the runner sleeps, so a test of a retry loop with a 30 second backoff finishes at once and gives the same delays every time. The jitter is pure (the builtin seeded `rand`), seeded from `Policy.seed` or from the clock at the start of the run. The circuit breaker and the limiters are pure state machines that take the current time as an argument, so they are deterministic too and fit any concurrency model.

An effect was chosen over passing `sleep` and `now` functions because a fake clock needs state that moves as the code sleeps, and a handler holds that state in one place, around any code, including operations that read the clock themselves.

## Example

```sspur
use retry.{FullJitter, simulate_hinted, exponential, breaker, default_breaker_config, call, Ran, Rejected}

type HttpError = Unavailable{retry_after_ms: Opt[Int]} | BadRequest

fn fetch(attempt: Int) -> Res[Str, HttpError] ! log
= do
  log("attempt {attempt}")
  if attempt == 1 then err(Unavailable{retry_after_ms: some(1500)}) else if attempt < 4 then err(Unavailable{retry_after_ms: none}) else ok("payload")

fn hint(e: HttpError) -> Opt[Int]
= match e
  | Unavailable{retry_after_ms} => retry_after_ms
  | BadRequest => none

fn main() -> Unit ! log
= do
  p = exponential(200, 5) with jitter := FullJitter, seed := some(7), budget_ms := some(10000)
  (o, end) = simulate_hinted(0, p, fetch, e => e != BadRequest, hint)
  log("{o.result} after {o.attempts} attempts, slept {o.delays} = {o.waited_ms} ms, {o.stop}, virtual clock at {end}")
  b = breaker(default_breaker_config() with failure_threshold := 2, cooldown_ms := 1000)
  (r1, b1) = call(b, 0, () => err("down"))
  (r2, b2) = call(b1, 10, () => err("down"))
  (r3, _) = call(b2, 20, () => ok("up"))
  log("{r1} {r2} {r3} {b2.state}")
```

Output (the same natively and with `--interp`; the virtual clock means nothing waits):

```
attempt 1
attempt 2
attempt 3
attempt 4
ok("payload") after 4 attempts, slept [1500, 6, 721] = 2227 ms, Succeeded, virtual clock at 2227
Ran{result: err("down")} Ran{result: err("down")} Rejected{wait_ms: 990} Open{until_ms: 1010}
```

A handler of your own decides what sleeping means, for example to record the delays:

```sspur
use retry.{clock, run, constant}

fn main() -> Unit ! log
= do
  var slept = []
  o = handle run(constant(250, 3), n => if n < 3 then err("busy") else ok(n))
    | retry_sleep(ms) => do
      slept := slept.push(ms)
      resume()
    | retry_now() => resume(0)
  log("{o.result} after sleeping {slept}")
```

```
ok(3) after sleeping [250, 250]
```

## API

### Policies

- `Policy`: `{max_attempts, base_ms, multiplier, max_delay_ms, jitter, budget_ms: Opt[Int], seed: Opt[Int]}`; change fields with `with` (`p with jitter := NoJitter, budget_ms := some(5000)`).
- `Jitter`: `NoJitter | FullJitter | EqualJitter | DecorrelatedJitter`, as in the AWS Architecture Blog's "Exponential Backoff And Jitter".
- `default_policy()`: 3 attempts, 100 ms doubling to 10 s, full jitter.
- `constant(delay_ms, attempts)`, `exponential(base_ms, attempts)` (doubling to 30 s, no jitter), `no_retry()`.
- `base_delay(p, attempt)`: the delay before jitter, `base_ms * multiplier ** (attempt - 1)` capped at `max_delay_ms`, without overflow.
- `next_delay(p, attempt, prev_ms, seed)`: one jittered delay and the next seed.
- `schedule(p)`: every delay of a run that always fails, from `p.seed` (or 0).
- `worst_case_ms(p)`: the most a run can sleep.

### Running

- `run(p, op)`, `run_when(p, op, retryable)`, `run_hinted(p, op, retryable, after)`: `op(attempt)` returns a `Res`; `retryable(e)` says whether an error is worth retrying; `after(e)` may give the delay the error asks for (an HTTP Retry-After), which replaces the computed one. They perform `clock` plus `op`'s effects.
- `run_now(p, op)`, `run_hinted_now(p, op, retryable, after)`: the same on the real clock (`! time`).
- `simulate(start_ms, p, op)`, `simulate_hinted(start_ms, p, op, retryable, after)`: the same on a virtual clock; they give the outcome and the virtual time at the end.
- `Outcome[T, E]`: `{result, attempts, delays, waited_ms, stop}`, with `Stop` = `Succeeded | Exhausted | NotRetryable | BudgetExceeded`.
- `clock`: the effect, with `retry_sleep(ms)` and `retry_now()`.

A run stops at the first success, at an error `retryable` rejects, after `max_attempts` attempts, or when the next delay would end past `budget_ms` from the start (the budget counts the time the operation itself takes, as the clock sees it).

### Circuit breaker

- `BreakerConfig`: `{failure_threshold, failure_rate, min_calls, window_ms, cooldown_ms, half_open_trials}`. The breaker opens when the last `window_ms` hold `failure_threshold` failures, or at least `min_calls` calls of which a `failure_rate` share failed; it stays open `cooldown_ms`, then lets `half_open_trials` trial calls through: if they all succeed it closes, and any failure opens it again.
- `default_breaker_config()`: 5 failures or half of 20 calls in 60 s, 30 s open, one trial.
- `breaker(config)`, `Breaker`, `BreakerState` (`Closed | Open{until_ms} | HalfOpen{started, succeeded}`).
- `allow(b, now)`, `record_success(b, now)`, `record_failure(b, now)`: the state machine, for code that makes the call itself.
- `call(b, now, op)`, `call_when(b, now, op, counts)`: run a call through the breaker; `Called[T, E]` is `Rejected{wait_ms} | Ran{result}`; with `counts` only some errors count as failures.
- `current_state(b, now)`, `ms_until_allowed(b, now)`, `window_counts(b, now)` (calls and failures in the window).

### Rate limiting

- `Bucket`, `bucket(capacity, per_sec, now)`: a full token bucket refilled continuously.
- `bucket_take(b, now, n)`, `bucket_tokens(b, now)`, `bucket_wait_ms(b, now, n)` (`none` when `n` exceeds the capacity).
- `Window`, `window_limiter(limit, window_ms)`: at most `limit` calls in any `window_ms`.
- `window_take(w, now)`, `window_remaining(w, now)`, `window_wait_ms(w, now)`.

A time before the last one seen refills nothing, so a clock that steps back never creates tokens.

## Out of scope

- Shared state across threads or processes: the breaker and limiters are values; keep one per caller, or behind an `Atomic`, a channel or a database row.
- Bulkheads, hedged requests and timeouts around a single attempt (SSPUR's `http.req` has `timeout_ms` and `retries` of its own for HTTP).
- Generic `with_system_clock(f)` and `simulate(start, f)` that take any thunk: a compiler bug currently stops a package from using its own effect in the function types of its parameters, so write the six-line handler shown above for custom cases.

## Install

```
sspur add retry
```

MIT licensed.
