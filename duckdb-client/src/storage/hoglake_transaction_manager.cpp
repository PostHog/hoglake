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
	try {
		hoglake_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
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
