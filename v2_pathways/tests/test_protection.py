import asyncio
import base64
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import zipfile

from fastapi import HTTPException
from starlette.requests import Request
import app


class Protection(unittest.TestCase):
    def test_missing_token_does_not_query(self):
        request = Request({"type": "http", "headers": []})
        with patch.object(app, "query_pathways") as query:
            with self.assertRaises(HTTPException) as error:
                asyncio.run(app.pathways(request))
            self.assertEqual(error.exception.status_code, 401)
            query.assert_not_called()

    def test_rejected_token_never_reads_private_data(self):
        error = urllib.error.HTTPError(app.UPSTREAM, 401, "Denied", {}, None)
        with patch("urllib.request.OpenerDirector.open", side_effect=error), patch.object(app, "build_pathways") as build:
            with self.assertRaises(HTTPException) as result:
                app.query_pathways("unused", {"schools": ["123165"]}, "invalid-test-token")
            self.assertEqual(result.exception.status_code, 401)
            build.assert_not_called()

    def test_upstream_canonical_filters_and_threshold_are_used(self):
        meta = {"filters": {"schools": ["123165"], "majors": ["42.01"]},
                "min_cell_weight": 25, "data_version": "upstream-release"}
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, _): return json.dumps({"meta": meta}).encode()
        with patch("urllib.request.OpenerDirector.open", return_value=Response()), patch.object(app, "build_pathways", return_value={"meta": {}, "pathways": {}}) as build:
            result = app.query_pathways("private-root", {"schools": ["123165"], "majors": ["untrusted"]}, "test-token")
            self.assertEqual(build.call_args.args, ("private-root", {**meta["filters"], "_pathway_min_cell_weight": 25}))
            self.assertEqual(result["meta"]["tracker_data_version"], "upstream-release")

    def test_mismatched_approval_fails_closed(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, _): return b'{"meta":{"filters":{"schools":["different"]}}}'
        with patch("urllib.request.OpenerDirector.open", return_value=Response()), patch.object(app, "build_pathways") as build:
            with self.assertRaises(HTTPException):
                app.query_pathways("unused", {"schools": ["123165"]}, "test-token")
            build.assert_not_called()

    def test_unsupported_school_is_not_substituted(self):
        with patch.object(app, "authorize", return_value=({"schools": ["other"]}, "v1")), patch.object(app, "build_pathways") as build:
            with self.assertRaises(HTTPException) as error:
                app.query_pathways("unused", {"schools": ["other"]}, "test-token")
            self.assertEqual(error.exception.status_code, 422)
            build.assert_not_called()

    def test_bundle_rejects_unexpected_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = io.BytesIO()
            with zipfile.ZipFile(archive, "w") as z:
                z.writestr("../../escape", "private")
                z.writestr("platform_manifest.json", "{}")
            bundle = Path(tmp) / "test.bundle"
            bundle.write_bytes(base64.b64encode(archive.getvalue()))
            with self.assertRaises(ValueError):
                app.unpack_bundle(bundle, Path(tmp) / "output")
            self.assertFalse((Path(tmp) / "output").exists())


if __name__ == "__main__":
    unittest.main()
