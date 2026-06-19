#include "hedge_c.h"

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include "db/database.h"
#include "io/io_executor.h"
#include "io/static_pool.h"
#include "key.h"
#include "manual_ctx.h"
#include "tmc/task.hpp"

struct hedge_value
{
    std::vector<std::byte> bytes;
};

struct hedge_db
{
    std::shared_ptr<hedge::db::database> db;
};

struct hedge_ctx
{
    std::shared_ptr<hedge::db::database> db;
    hedge::db_ctx ctx;
    std::deque<hedge_outcome> outcomes;
    uint64_t next_seq{0};
};

namespace
{
    std::vector<std::byte> to_bytes(const uint8_t* p, size_t n)
    {
        const auto* b = reinterpret_cast<const std::byte*>(p);
        return std::vector<std::byte>(b, b + n);
    }

    int32_t to_status_code(const hedge::error& e)
    {
        return static_cast<int32_t>(e.code()) + 1;
    }

    tmc::task<hedge::expected<std::vector<std::byte>>>
    get_task(std::shared_ptr<hedge::db::database> db, std::vector<std::byte> key_bytes)
    {
        hedge::key_t key{key_bytes.data(), key_bytes.size()};
        co_return co_await db->get_async(key);
    }
} // namespace

extern "C"
{
    hedge_db* hedge_open(const char* path, int create, const hedge_config* cfg)
    {
        try
        {
            // The process-wide pool backs background work and a couple of config defaults; init once.
            static std::once_flag pool_once;
            std::call_once(pool_once, []
                           { hedge::io::static_pool::instance()->init(hedge::io::executor_config{
                                 .name = "hedge-capi",
                                 .queue_depth = 16,
                                 .n_threads = size_t{1},
                             }); });

            hedge::db::db_config dbc{};
            if(cfg != nullptr)
            {
                if(cfg->num_writer_threads > 0)
                    dbc.num_writer_threads = cfg->num_writer_threads;
                if(cfg->memtable_budget_bytes > 0)
                    dbc.memtable_budget_bytes = cfg->memtable_budget_bytes;
                if(cfg->num_partition_exponent > 0)
                    dbc.num_partition_exponent = cfg->num_partition_exponent;
                if(cfg->bucket_ratio > 0.0)
                    dbc.bucket_ratio = cfg->bucket_ratio;
                if(cfg->compaction_read_ahead_bytes > 0)
                    dbc.compaction_read_ahead_size_bytes = cfg->compaction_read_ahead_bytes;
                if(cfg->max_pending_flushes > 0)
                    dbc.max_pending_flushes = cfg->max_pending_flushes;
                if(cfg->min_merge_width > 0)
                    dbc.min_merge_width = cfg->min_merge_width;
                if(cfg->max_merge_width > 0)
                    dbc.max_merge_width = cfg->max_merge_width;
                if(cfg->index_page_cache_bytes > 0)
                    dbc.index_page_clock_cache_size_bytes = cfg->index_page_cache_bytes;
                if(cfg->num_background_workers > 0)
                    dbc.num_background_workers = cfg->num_background_workers;
                dbc.use_direct_io = (cfg->disable_direct_io == 0);
                dbc.disable_wal = (cfg->disable_wal != 0);
                dbc.auto_compaction = (cfg->disable_auto_compaction == 0);
                if(cfg->disable_l0_backpressure != 0)
                    dbc.ssts_in_l0_block_write_threshold = std::nullopt;
            }

            auto res = create ? hedge::db::database::make_new(path, dbc)
                              : hedge::db::database::load(path, dbc);
            if(!res)
                return nullptr;

            return new hedge_db{std::move(res.value())};
        }
        catch(...)
        {
            return nullptr;
        }
    }

    void hedge_close(hedge_db* db)
    {
        delete db;
    }

    void hedge_wait_for_compactions(hedge_db* db)
    {
        try
        {
            db->db->wait_for_compactions_to_finish();
        }
        catch(...)
        {
        }
    }

    hedge_ctx* hedge_ctx_create(hedge_db* db, uint32_t queue_depth)
    {
        try
        {
            auto* h = new hedge_ctx{};
            h->db = db->db;
            h->ctx.init(queue_depth);
            return h;
        }
        catch(...)
        {
            return nullptr;
        }
    }

    void hedge_ctx_destroy(hedge_ctx* ctx)
    {
        if(ctx == nullptr)
            return;
        ctx->ctx.shutdown();
        delete ctx;
    }

    int hedge_put(hedge_db* db, const uint8_t* key, size_t klen,
                  const uint8_t* val, size_t vlen)
    {
        hedge::key_t k{key, klen};
        auto s = db->db->put(k, std::span<const std::byte>{reinterpret_cast<const std::byte*>(val), vlen});
        return s ? HEDGE_OK : to_status_code(s.error());
    }

    int hedge_remove(hedge_db* db, const uint8_t* key, size_t klen)
    {
        hedge::key_t k{key, klen};
        auto s = db->db->remove(k);
        return s ? HEDGE_OK : to_status_code(s.error());
    }

    uint64_t hedge_submit_get(hedge_ctx* ctx, const uint8_t* key, size_t klen)
    {
        uint64_t seq = ctx->next_seq++;
        ctx->ctx.post<hedge::expected<std::vector<std::byte>>>(
            get_task(ctx->db, to_bytes(key, klen)),
            [ctx, seq](hedge::expected<std::vector<std::byte>> result)
            {
                hedge_outcome o{};
                o.seq = seq;
                if(result)
                {
                    o.status = HEDGE_OK;
                    o.value = new hedge_value{std::move(result.value())};
                }
                else
                {
                    o.status = to_status_code(result.error());
                    o.value = nullptr;
                }
                ctx->outcomes.push_back(o);
            });
        return seq;
    }

    size_t hedge_tick(hedge_ctx* ctx)
    {
        return ctx->ctx.tick();
    }

    int hedge_outcome_pop(hedge_ctx* ctx, hedge_outcome* out)
    {
        if(ctx->outcomes.empty())
            return 0;

        *out = ctx->outcomes.front();
        ctx->outcomes.pop_front();
        return 1;
    }

    const uint8_t* hedge_value_data(const hedge_value* v)
    {
        return reinterpret_cast<const uint8_t*>(v->bytes.data());
    }

    size_t hedge_value_len(const hedge_value* v)
    {
        return v->bytes.size();
    }

    void hedge_value_free(hedge_value* v)
    {
        delete v;
    }
} // extern "C"
