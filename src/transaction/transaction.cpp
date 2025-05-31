// src/transaction/transaction.cpp
#include "transaction/transaction.h"

namespace SimpleRDBMS {

Transaction::Transaction(txn_id_t txn_id, IsolationLevel isolation_level)
    : txn_id_(txn_id),
      state_(TransactionState::GROWING),
      isolation_level_(isolation_level),
      prev_lsn_(INVALID_LSN) {
}

Transaction::~Transaction() = default;

void Transaction::AddToWriteSet(const RID& rid, const Tuple& tuple) {
    // Only store the first version (for rollback)
    if (write_set_.find(rid) == write_set_.end()) {
        write_set_[rid] = tuple;
    }
}

}  // namespace SimpleRDBMS