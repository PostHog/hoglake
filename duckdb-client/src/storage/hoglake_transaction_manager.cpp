#include "storage/hoglake_transaction_manager.hpp"

namespace duckdb {

HoglakeTransactionManager::HoglakeTransactionManager(AttachedDatabase &db_p, HoglakeCatalog &hoglake_catalog_p)
    : TransactionManager(db_p), hoglake_catalog(hoglake_catalog_p) {
}

Transaction &HoglakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_shared_ptr<HoglakeTransaction>(hoglake_catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData HoglakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &hoglake_transaction = transaction.Cast<HoglakeTransaction>();
	ErrorData result;
	try {
		hoglake_transaction.Commit(context);
	} catch (std::exception &ex) {
		result = ErrorData(ex);
	}
	// erase on BOTH paths: DuckDB does not call RollbackTransaction for
	// a transaction whose commit returned an error, so keeping it in the
	// map would leak it (and its catalog entry caches) until DETACH
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return result;
}

void HoglakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &hoglake_transaction = transaction.Cast<HoglakeTransaction>();
	hoglake_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void HoglakeTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// hoglake maintenance (compaction/expiry/cleanup) is server-side;
	// CHECKPOINT is a no-op for now (maintenance passthrough functions
	// land in M5).
}

} // namespace duckdb
