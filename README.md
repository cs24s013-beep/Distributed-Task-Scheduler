# Thread pool with a bounded queue

Four worker threads pull requests from a queue that holds 64 items. When the
queue is full, new requests are refused instead of queued. The program sends
requests at increasing rates and measures how long they wait.

C++17, single file, no dependencies.

## Run it

```
g++ -O2 -std=c++17 -pthread scheduler.cpp -o scheduler
./scheduler
```

`-O2` is required, otherwise the timings mean nothing. Leave at least one core
free for the load generator.

## How it works

- A fixed pool of `std::thread` workers, created once, all pulling from one
  shared queue.
- The queue is hand-written over `std::mutex` and `std::condition_variable`.
  `try_push` returns false when full rather than blocking, and that refusal is
  the admission control.
- Each request is a fixed-iteration hash loop, so the work is real CPU time.
- A separate thread issues requests on a fixed schedule and does not wait for
  replies.
- Arrival, start and finish times are recorded per request; the program sorts
  them at the end and prints percentiles.

## Results

MacBook Air, 8 cores, macOS. 4 workers, queue of 64, 3-second trials.
One request takes 0.573 ms, so four threads should handle about 6,980 per
second. Median of three runs, per column. Times are milliseconds.

| offered/s | completed/s | refused | wait p50 | wait p99 | total p50 | total p99 |
|---|---|---|---|---|---|---|
| 3,490 | 3,489 | 0 | 0.00 | 2.71 | 0.63 | 3.76 |
| 5,584 | 5,562 | 0 | 0.01 | 8.15 | 0.67 | 8.77 |
| 6,980 | 6,222 | 2,229 | 9.98 | 11.17 | 10.61 | 11.90 |
| 10,470 | 6,263 | 12,324 | 10.05 | 10.61 | 10.69 | 11.28 |
| 13,960 | 6,275 | 22,848 | 10.00 | 10.64 | 10.63 | 11.38 |
| 20,940 | 6,248 | 43,858 | 10.00 | 10.78 | 10.63 | 11.48 |

## What it shows

**Throughput stops at about 6,250 per second.** Offering three times that much
produces no extra completed work. The 6,980 estimate came from timing one thread
on an idle machine, so it was always an upper bound.

**Once the queue fills, waiting time jumps and then stays put.** It goes from
0.01 ms to about 10 ms between 5,600 and 7,000 requests per second, then barely
moves however hard you push. The extra load leaves as refusals instead — 9%
refused at the cliff, 70% refused at the top — while latency holds steady. That
is the point of bounding the queue.

**The 10 ms is predictable.** A full queue of 64 items, drained by 4 threads at
0.573 ms each, is 64 × 0.573 ÷ 4 = 9.2 ms. Measured p50 wait, every saturated
row, all three runs: 9.92 to 10.14 ms.

**The saturated rows repeat; the light-load rows do not.** Across three runs the
saturated numbers agree within about 2%. Below capacity they do not: p99 wait at
5,600/s was 0.02 ms, 8.15 ms, and 12.80 ms on three identical runs.

**There is a clue about why.** One run refused 62 requests at 5,600/s, well
below capacity, where the others refused none. Overflowing a 64-slot queue at
that arrival rate needs about 11.5 ms of traffic to arrive at once — and that
run's p99 wait was 12.80 ms. So the cause is not a small per-request cost; it is
something stalling for roughly 12 ms, letting the queue fill.

The suspect is the load generator rather than the pool. It is one unpinned
thread on a laptop, and if the OS deschedules it for 12 ms it will fire every
missed request back to back when it resumes, overflowing the queue by itself.
That would make this a flaw in the measurement, not in the thing measured. The
data so far cannot tell the two apart.

## Notes

- Single machine, single process. Nothing is distributed.
- Work is a CPU loop, not `sleep`. Sleeping would only measure sleeping.
- Requests are sent on a fixed schedule and do not wait for replies. Waiting for
  each reply before sending the next would throttle the test and hide the queue
  filling up.
- `arena.reserve()` before the run is load-bearing: workers hold pointers into
  that vector, and one reallocation would dangle every outstanding one.
- Medians are taken per column, so a row is not a single run.
- Light-load p99 figures should not be quoted as fixed numbers. They vary by
  more than 100× between runs.

## Next

- Log the gap between consecutive arrival times and look for a ~12 ms pause
  followed by a burst. That settles whether the stall is in the generator or the
  pool.
- Try a queue of 8 and of 1024. Waiting time should scale with queue size. You
  can have fewer refusals or a shorter wait, not both.
- One queue per worker instead of one shared queue, and compare.
