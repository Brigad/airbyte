#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import docker
import pytest

pytest_plugins = ("source_acceptance_test.plugin",)


@pytest.fixture(scope="session", autouse=True)
def connector_setup():
    client = docker.from_env()
    container = client.containers.run("airbyte/source-aircall", detach=True)
    yield
    container.stop()
