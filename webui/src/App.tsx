import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { CatalogsPage } from "./pages/CatalogsPage";
import { CatalogPage } from "./pages/CatalogPage";
import { CentralMaintenancePage } from "./pages/CentralMaintenancePage";
import { NamespacePage } from "./pages/NamespacePage";
import { TablePage } from "./pages/TablePage";
import { ConsumersPage } from "./pages/ConsumersPage";
import { MaintenancePage } from "./pages/MaintenancePage";
import { MetricsPage } from "./pages/MetricsPage";
import { PartitionsPage } from "./pages/PartitionsPage";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<CatalogsPage />} />
        <Route path="/metrics" element={<MetricsPage />} />
        <Route path="/maintenance" element={<CentralMaintenancePage />} />
        <Route path="/catalogs/:catalog" element={<CatalogPage />} />
        <Route path="/catalogs/:catalog/consumers" element={<ConsumersPage />} />
        <Route
          path="/catalogs/:catalog/partitions"
          element={<PartitionsPage />}
        />
        <Route
          path="/catalogs/:catalog/maintenance"
          element={<MaintenancePage />}
        />
        <Route
          path="/catalogs/:catalog/namespaces/:namespace"
          element={<NamespacePage />}
        />
        <Route
          path="/catalogs/:catalog/namespaces/:namespace/tables/:table"
          element={<TablePage />}
        />
        <Route path="*" element={<p className="empty">Not found.</p>} />
      </Route>
    </Routes>
  );
}
