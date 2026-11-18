import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getNamespace } from "../api/client";
import type { Namespace, Table } from "../api/types";

/**
 * Whether a namespace is a remote schema: read-only, served by another
 * Hoglake through this catalog, which relays only get-namespace, list
 * tables, get-table and scan for it.
 *
 * Remote exactly when the server says so, never from the name: the
 * catalog's listing entry, the get-namespace answer or a table answer of
 * the namespace carries `remote: true`. Any one is enough, and the
 * listing and table answers are read from the cache, never requested.
 * Local needs the get-namespace answer, because a caller holding `*` for
 * teams gets no team schema in the listing yet can open one by URL.
 * While neither is known (pending, or get-namespace failed), a page
 * requests and offers nothing that depends on the answer.
 */
export function useSchemaKind(
  catalog: string | undefined,
  namespace: string | undefined,
): { remote: boolean; local: boolean; error: Error | null } {
  const client = useQueryClient();
  const answer = useQuery({
    queryKey: ["namespace", catalog, namespace],
    queryFn: () => getNamespace(catalog!, namespace!),
    enabled: Boolean(catalog && namespace),
    staleTime: Infinity,
  });
  const listed = client
    .getQueryData<Namespace[]>(["namespaces", catalog])
    ?.some((ns) => ns.name === namespace && ns.remote === true);
  const tables = client
    .getQueriesData<Table>({ queryKey: ["table", catalog, namespace] })
    .some(([, t]) => t?.remote === true);
  const remote = answer.data?.remote === true || listed === true || tables;
  return { remote, local: !remote && answer.isSuccess, error: answer.error };
}
