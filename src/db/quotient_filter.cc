
#include <error.hpp>

#include "qf.h"
#include "quotient_filter.h"

namespace hedge::db
{
    // C++ Wrapper for the quotient filter implementation

    quotient_filter::~quotient_filter()
    {
        if(this->_qf_impl.qf_table != nullptr)
            third_party::qf_destroy(&this->_qf_impl);
    }

    bool quotient_filter::insert(uint64_t hash)
    {
        return third_party::qf_insert(&this->_qf_impl, hash);
    }

    bool quotient_filter::may_contain(uint64_t hash)
    {
        return third_party::qf_may_contain(&this->_qf_impl, hash);
    }

    bool quotient_filter::remove(uint64_t hash)
    {
        return third_party::qf_remove(&this->_qf_impl, hash);
    }

    void quotient_filter::clear()
    {
        third_party::qf_clear(&this->_qf_impl);
    }

    hedge::expected<quotient_filter> quotient_filter::make(uint32_t q, uint32_t r)
    {
        third_party::quotient_filter qf_inner;

        bool ok = third_party::qf_init(&qf_inner, q, r);

        if(!ok)
            return hedge::error("Could not initialize the QF. q+r should be <= 64; also, check the memory");

        quotient_filter qf{};
        qf._qf_impl = qf_inner;

        return qf;
    }
} // namespace hedge::db