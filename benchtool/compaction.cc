#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include "db/database.h"
#include "modes.h"
#include "utils.h"

namespace hedge::db
{
    void run_compaction(const std::shared_ptr<database>& db, const values_t& values,
                        size_t n, size_t vsize, size_t num_threads, bool measure_latency)
    {
        // Load phase: data accumulates in L0 SSTs (auto_compaction disabled for this mode).
        // run_load ends with wait_for_compactions_to_finish(), so the memtable is fully
        // flushed and everything sits in L0 before we compact.
        run_load(db, values, n, vsize, num_threads, measure_latency);

        using clk = std::chrono::high_resolution_clock;
        std::cout << "\nScheduling compaction...\n";
        db->trigger_compaction(/*compact_all=*/false);

        // trigger_compaction only posts a signal to the async compaction worker.
        // Give it a moment to pull the signal and register the pending SSTs, otherwise
        // wait_for_compactions_to_finish() can observe a zero pending count and return
        // before the compaction has actually started. Keep this short: it must outlast
        // the signal hand-off (microseconds) but not the compaction itself.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        auto t0 = clk::now();
        db->wait_for_compactions_to_finish();
        double elapsed = std::chrono::duration<double>(clk::now() - t0).count();

        print_throughput("compaction", n, elapsed, vsize);
    }

} // namespace hedge::db
