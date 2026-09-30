#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""Unit tests for RAGFlowGCS prefix_path handling."""

import importlib
from unittest.mock import Mock

import pytest

import common.settings  # noqa: F401
from rag.utils import gcs_conn


def _new_storage(monkeypatch, config):
    module = importlib.reload(gcs_conn)
    client = Mock()
    monkeypatch.setattr(module.settings, "GCS", config)
    monkeypatch.setattr(module.storage, "Client", Mock(return_value=client))
    return module.RAGFlowGCS(), client


@pytest.mark.parametrize(
    "config, expected",
    [
        ({"bucket": "b"}, "kb1/doc.pdf"),
        ({"bucket": "b", "prefix_path": ""}, "kb1/doc.pdf"),
        ({"bucket": "b", "prefix_path": "ragflow"}, "ragflow/kb1/doc.pdf"),
        ({"bucket": "b", "prefix_path": "/ragflow/"}, "ragflow/kb1/doc.pdf"),
    ],
)
def test_put_uses_prefix_path(monkeypatch, config, expected):
    store, client = _new_storage(monkeypatch, config)

    assert store.put("kb1", "doc.pdf", b"x")

    client.bucket.assert_called_with("b")
    client.bucket.return_value.blob.assert_called_with(expected)


def test_remove_bucket_lists_under_prefix(monkeypatch):
    store, client = _new_storage(monkeypatch, {"bucket": "b", "prefix_path": "ragflow"})
    client.list_blobs.return_value = []

    store.remove_bucket("kb1")

    client.list_blobs.assert_called_with("b", prefix="ragflow/kb1/")
