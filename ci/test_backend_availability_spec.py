"""Resolve actual OpenAPI responses; no new wire fields or client retry policy."""

import unittest

import yaml
from check_hoglite_results import ROOT


class AvailabilitySpecTest(unittest.TestCase):
    def setUp(self):
        self.spec = yaml.safe_load(
            (ROOT / "server/src/main/resources/openapi/hoglake.yaml").read_text()
        )
        self.operations = {
            op["operationId"]: (method, op)
            for item in self.spec["paths"].values()
            for method, op in item.items()
            if isinstance(op, dict) and "operationId" in op
        }

    def response(self, operation):
        response = self.operations[operation][1]["responses"]["503"]
        if "$ref" in response:
            parts = response["$ref"].split("/")
            self.assertEqual(parts[:3], ["#", "components", "responses"])
            response = self.spec["components"]["responses"][parts[3]]
        return response

    def test_every_real_backend_route_and_no_static_or_unimplemented_route(self):
        excluded = {
            "livez",
            "healthz",
            "metrics",
            "getInstanceInfo",
            "decodeBound",
            "listPublications",
            "createPublication",
            "getPublication",
            "dropPublication",
            "exportCatalog",
        }
        for name, (method, operation) in self.operations.items():
            if name in excluded:
                if name == "healthz":
                    self.assertNotIn("content", self.response(name))
                else:
                    self.assertNotIn("503", operation["responses"])
                continue
            response = self.response(name)
            self.assertIn("backend_unavailable", response["description"], name)
            self.assertIn("Retry-After", response["headers"], name)
            schema = response["content"]["application/json"]["schema"]
            if name == "lookupPartition":
                self.assertEqual(
                    {item["$ref"] for item in schema["anyOf"]},
                    {
                        "#/components/schemas/PartitionLookupUnavailableError",
                        "#/components/schemas/ApiError",
                    },
                )
            else:
                self.assertEqual(schema["$ref"], "#/components/schemas/ApiError")
            if method in {"get"} or name == "lookupPartition":
                self.assertNotIn(
                    "commit_outcome_unknown", response["description"], name
                )
            else:
                self.assertIn("commit_outcome_unknown", response["description"], name)
        self.assertIn(
            "partition_lookup_unavailable",
            self.response("lookupPartition")["description"],
        )

    def test_all_six_commit_responses_preserve_queue_and_uncertainty(self):
        commits = [
            name
            for name in self.operations
            if name.startswith("commit") and name != "commitConsumerOffset"
        ]
        self.assertEqual(len(commits), 6)
        for name in commits:
            response = self.response(name)
            self.assertIn("commit_queue_timeout", response["description"])
            self.assertIn("commit_outcome_unknown", response["description"])
            self.assertIn("retained receipt", response["description"])
            self.assertEqual(
                response["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ApiError",
            )
        for name in [
            "prepareTableCreation",
            "getTableCreation",
            "preparePartitionedTableCreation",
            "prepareSortedTableCreation",
            "prepareMetadataTableCreation",
            "publishTableCreation",
            "publishTableCreationUploads",
            "abortTableCreation",
        ]:
            self.assertIn("same operation ID", self.response(name)["description"])
            self.assertIn("commit_queue_timeout", self.response(name)["description"])

    def test_open_error_string_and_conditional_recovery_contract(self):
        error = self.spec["components"]["schemas"]["ApiError"]
        self.assertEqual(error["properties"]["error"]["type"], "string")
        self.assertNotIn("enum", error["properties"]["error"])
        self.assertEqual(
            set(error["properties"]),
            {"error", "detail", "tables", "read_snapshot", "retry", "reason"},
        )
        prose = self.spec["info"]["description"]
        for phrase in [
            "may or may not have committed",
            "does not prove whole-operation rollback",
            "original idempotency key and identical",
            "original catalog incarnation and retained receipt",
            "missing receipt does not prove",
            "ordinary DDL have no general client replay guarantee",
            "not a REST replay token",
        ]:
            self.assertIn(phrase, prose)
        receipt = self.operations["getCommitReceipt"][1]["description"]
        self.assertNotIn("no expiration", receipt)
        self.assertIn("configured receipt retention", receipt)
        key_prose = self.spec["components"]["schemas"]["CommitRequest"]["properties"][
            "idempotency_key"
        ]["description"]
        self.assertNotIn("retained indefinitely", key_prose)
        self.assertIn("configured receipt retention", key_prose)


if __name__ == "__main__":
    unittest.main()
