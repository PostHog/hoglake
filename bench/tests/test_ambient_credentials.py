"""Running bench in-cluster means letting AWS supply the credentials.

A pod under IRSA has no keys to pass: the web-identity token is on disk and
the SDK's credential chain picks it up. Bench's defaults point at the local
MinIO stack, so the in-cluster case is expressed by emptying those settings
rather than by inventing new ones.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from hoglake_bench.context import Bench, BenchConfig


def _s3_config_for(**overrides):
    cfg = BenchConfig(**overrides)
    with patch("hoglake_bench.context.HoglakeClient") as client:
        Bench(cfg)
    return client.call_args.kwargs["s3"]


class TestCredentialPassing:
    def test_empty_settings_defer_to_the_ambient_chain(self) -> None:
        s3 = _s3_config_for(s3_endpoint="", s3_access_key="", s3_secret_key="")
        # None, not "": pyhoglake omits the argument entirely when it is None,
        # which is what makes pyarrow consult the credential chain.
        assert s3.access_key is None
        assert s3.secret_key is None
        assert s3.endpoint_override is None
        # The region still travels — the chain supplies credentials, not placement.
        assert s3.region == "us-east-1"

    def test_the_local_stack_still_gets_its_keys(self) -> None:
        s3 = _s3_config_for()
        assert s3.access_key == "hoglake"
        assert s3.secret_key == "hoglake123"
        assert s3.endpoint_override == "http://localhost:9000"

    def test_bucket_creation_is_only_offered_to_the_local_stack(self) -> None:
        assert _s3_config_for().allow_bucket_creation is True
        assert _s3_config_for(s3_endpoint="").allow_bucket_creation is False


class TestEnsureBucket:
    def test_creates_the_bucket_against_a_local_endpoint(self) -> None:
        cfg = BenchConfig(bucket="hoglake-bench")
        with patch("hoglake_bench.context.HoglakeClient"):
            bench = Bench(cfg)
        fs = MagicMock()
        bench.client._filesystem.return_value = fs
        bench.ensure_bucket()
        fs.create_dir.assert_called_once_with("hoglake-bench")

    def test_never_asks_aws_to_create_a_bucket_it_does_not_own(self) -> None:
        # The IRSA policy carries no s3:CreateBucket, and the bucket already
        # exists: asking would fail the run before the first row.
        cfg = BenchConfig(s3_endpoint="", bucket="posthog-gigahog-mw-dev")
        with patch("hoglake_bench.context.HoglakeClient"):
            bench = Bench(cfg)
        fs = MagicMock()
        bench.client._filesystem.return_value = fs
        bench.ensure_bucket()
        fs.create_dir.assert_not_called()
