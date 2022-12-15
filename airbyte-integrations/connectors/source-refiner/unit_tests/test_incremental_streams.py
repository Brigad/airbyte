#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from pytest import fixture
from source_refiner.source import IncrementalRefinerStream


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalRefinerStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalRefinerStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalRefinerStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = IncrementalRefinerStream()
    expected_cursor_field = ["last_shown_at"]
    assert stream.cursor_field == expected_cursor_field


# def test_get_updated_state(patch_incremental_base_class):
#     stream = IncrementalRefinerStream(start_date="2022-11-29T00:00:00Z", time_window={ "days": 1 })
#     inputs = {"current_stream_state": None, "latest_record": None}
#     # TODO: replace this with your expected updated stream state
#     expected_state = {}
#     assert stream.get_updated_state(**inputs) == expected_state


# def test_stream_slices(patch_incremental_base_class):
#     stream = IncrementalRefinerStream(start_date="2022-11-29T00:00:00Z", time_window={ "days": 1 })
#     # TODO: replace this with your input parameters
#     inputs = {"sync_mode": SyncMode.incremental, "cursor_field": ['created_at'], "stream_state": { "created_at": 1500 }}
#     # TODO: replace this with your expected stream slices list
#     expected_stream_slice = [None]
#     assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalRefinerStream, "cursor_field", "dummy_field")
    stream = IncrementalRefinerStream()
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = IncrementalRefinerStream()
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class):
    stream = IncrementalRefinerStream()
    # TODO: replace this with your expected checkpoint interval
    expected_checkpoint_interval = None
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
