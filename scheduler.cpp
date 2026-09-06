// A fixed worker pool behind a bounded queue, driven at a controlled request
// rate, measuring queue wait and total latency at p50 and p99.
//
// Build:  g++ -O2 -std=c++17 -pthread scheduler.cpp -o scheduler
// Run:    ./scheduler
//
// -O2 is not optional. Without it the work loop runs at a different speed than
// anything you would ever ship, and every number below is meaningless.
//
// The point is the table it prints. Watch p99 rise long before throughput
// moves, and watch the bounded queue trade rejections for stable latency
// once offered load passes capacity.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using Nanos = std::int64_t;

// --- knobs worth turning ---
constexpr int WORKERS        = 4;        // set to (cores - 1); the load generator needs one
constexpr int QUEUE_CAPACITY = 64;       // try 8, then 1024, and compare p99 vs rejections
constexpr int WORK_ITERATIONS = 400000;  // roughly 1ms of work; measured at startup
constexpr int TRIAL_SECONDS  = 3;

// Written by every worker so the optimiser cannot delete the work loop.
volatile unsigned long long sink;

static Nanos now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch()).count();
}

// --- a single request and its timestamps ---
struct Request {
    int   id;
    Nanos arrival;
    Nanos start = 0;   // when a worker picked it up
    Nanos end   = 0;   // when the worker finished

    Nanos queue_wait() const { return start - arrival; }
    Nanos total()      const { return end - arrival; }
};

// CPU-bound work. Not sleep() — sleep measures the ability to sleep.
static void burn(int iterations) {
    unsigned long long h = 1;
    for (int i = 0; i < iterations; ++i) {
        h = h * 31 + (static_cast<unsigned long long>(i) ^ (h >> 7));
    }
    sink = h;
}

// How long one request actually takes on this machine.
static double measure_service_millis() {
    for (int i = 0; i < 200; ++i) burn(WORK_ITERATIONS);   // settle caches, discard
    Nanos t0 = now();
    for (int i = 0; i < 100; ++i) burn(WORK_ITERATIONS);
    return (now() - t0) / 100 / 1e6;
}

// A bounded queue whose push FAILS instead of blocking when full.
// That failure is the admission control: we refuse work rather than let the
// queue grow without bound. The standard library has no such container, so it
// is about thirty lines of mutex and condition variable.
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    bool try_push(Request* r) {
        {
            std::lock_guard<std::mutex> lock(m_);
            if (q_.size() >= capacity_) return false;   // full -> reject
            q_.push(r);
        }
        cv_.notify_one();
        return true;
    }

    // Blocks until an item is available, or returns false once closed and drained.
    bool pop(Request*& out) {
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [this] { return !q_.empty() || closed_; });
        if (q_.empty()) return false;
        out = q_.front();
        q_.pop();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(m_);
            closed_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex              m_;
    std::condition_variable cv_;
    std::queue<Request*>    q_;
    std::size_t             capacity_;
    bool                    closed_ = false;
};

struct Result {
    int    offered_rps = 0;
    int    completed   = 0;
    int    rejected    = 0;
    double p50_wait = 0, p99_wait = 0, p50_total = 0, p99_total = 0;
    double achieved_rps = 0;
};

static double percentile_millis(std::vector<Nanos>& sorted, double p) {
    if (sorted.empty()) return std::nan("");
    int idx = static_cast<int>(std::ceil(p / 100.0 * sorted.size())) - 1;
    idx = std::max(0, std::min(idx, static_cast<int>(sorted.size()) - 1));
    return sorted[idx] / 1e6;
}

// Park until the deadline, spinning for the last fraction of a millisecond.
static void wait_until(Nanos deadline) {
    for (;;) {
        Nanos remaining = deadline - now();
        if (remaining <= 0) return;
        if (remaining > 1000000) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(remaining - 500000));
        } else {
            std::this_thread::yield();
        }
    }
}

static Result run_trial(int requests_per_second) {
    BoundedQueue queue(QUEUE_CAPACITY);

    // Each worker collects its own completions, so the hot path takes no
    // second lock. Merged after the join.
    std::vector<std::vector<Request*>> done(WORKERS);
    std::vector<std::thread> workers;
    workers.reserve(WORKERS);

    for (int w = 0; w < WORKERS; ++w) {
        workers.emplace_back([&queue, &done, w] {
            Request* r = nullptr;
            while (queue.pop(r)) {
                r->start = now();
                burn(WORK_ITERATIONS);
                r->end = now();
                done[w].push_back(r);
            }
        });
    }

    const int total = requests_per_second * TRIAL_SECONDS;

    // Reserved exactly, so no reallocation happens and the pointers we hand
    // to workers stay valid for the whole trial.
    std::vector<Request> arena;
    arena.reserve(total);

    int rejected = 0;
    const Nanos interval = 1000000000LL / requests_per_second;
    const Nanos start_wall = now();

    // Open loop: requests arrive on a schedule and do not wait for replies.
    // A closed loop (send, wait for the response, send again) throttles itself
    // and hides exactly the queueing this program is meant to show.
    for (int i = 0; i < total; ++i) {
        wait_until(start_wall + i * interval);
        arena.push_back(Request{i, now()});
        if (!queue.try_push(&arena.back())) {
            ++rejected;
        }
    }

    queue.close();
    for (auto& t : workers) t.join();
    const double elapsed_sec = (now() - start_wall) / 1e9;

    std::vector<Nanos> waits, totals;
    for (auto& per_worker : done) {
        for (Request* r : per_worker) {
            waits.push_back(r->queue_wait());
            totals.push_back(r->total());
        }
    }
    std::sort(waits.begin(), waits.end());
    std::sort(totals.begin(), totals.end());

    Result res;
    res.offered_rps  = requests_per_second;
    res.completed    = static_cast<int>(waits.size());
    res.rejected     = rejected;
    res.p50_wait     = percentile_millis(waits, 50);
    res.p99_wait     = percentile_millis(waits, 99);
    res.p50_total    = percentile_millis(totals, 50);
    res.p99_total    = percentile_millis(totals, 99);
    res.achieved_rps = res.completed / elapsed_sec;
    return res;
}

int main() {
    std::printf("cores=%u  workers=%d  queue=%d\n",
                std::thread::hardware_concurrency(), WORKERS, QUEUE_CAPACITY);

    const double service_ms = measure_service_millis();
    const int capacity = static_cast<int>(WORKERS * 1000.0 / service_ms);
    std::printf("service time = %.3f ms/request  ->  theoretical capacity ~%d req/s\n\n",
                service_ms, capacity);

    run_trial(std::max(1, capacity / 2));   // warm-up trial, result discarded

    std::printf("%-10s %-10s %-9s %-9s %-9s %-9s %-9s\n",
                "offered", "done/s", "rejected", "wait p50", "wait p99", "tot p50", "tot p99");
    std::printf("--------------------------------------------------------------------------\n");

    const double load_factors[] = {0.5, 0.8, 1.0, 1.5, 2.0, 3.0};
    for (double f : load_factors) {
        int rps = std::max(1, static_cast<int>(capacity * f));
        Result r = run_trial(rps);
        std::printf("%-10d %-10.0f %-9d %-9.2f %-9.2f %-9.2f %-9.2f\n",
                    r.offered_rps, r.achieved_rps, r.rejected,
                    r.p50_wait, r.p99_wait, r.p50_total, r.p99_total);
    }

    std::printf("\nLatencies are milliseconds. Things to try:\n"
                "  - QUEUE_CAPACITY 8 vs 1024: more rejections, or a worse p99. Pick one.\n"
                "  - WORKERS above core count: throughput flat, p99 worse.\n"
                "  - One queue per worker instead of one shared queue, and compare.\n"
                "  - Run each row three times and take the median before believing it.\n");
    return 0;
}
