#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/hoglake_transaction.hpp"
#include "storage/hoglake_catalog.hpp"

namespace duckdb {

class HoglakeTransactionManager : public TransactionManager {
public:
	HoglakeTransactionManager(AttachedDatabase &db, HoglakeCatalog &hoglake_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	HoglakeCatalog &hoglake_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, shared_ptr<HoglakeTransaction>> transactions;
};

} // namespace duckdb
