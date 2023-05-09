#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class RefinerStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class RefinerStream(HttpStream, ABC)` which is the current class
    `class Customers(RefinerStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(RefinerStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalRefinerStream((RefinerStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://api.refiner.io/v1/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.json()["pagination"]["current_page"] == response.json()["pagination"]["last_page"]:
            return None
        return {"page": response.json()["pagination"]["current_page"] + 1}

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        return 5

    def path(self, *args, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return self.name

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        params = {"page_length": 1000, "include": "all"}
        if stream_state and stream_state[self.cursor_field]:
            params.update({"date_range_start": stream_state[self.cursor_field]})
        if next_page_token:
            params.update(next_page_token)
        else:
            params["page"] = 1
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield from response.json()["items"]


# Basic incremental stream
class IncrementalRefinerStream(RefinerStream, IncrementalMixin):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._cursor_value = None

    @property
    @abstractmethod
    def cursor_field(self) -> str:
        pass

    def _update_state(self, latest_cursor):
        if latest_cursor:
            new_state = max(latest_cursor, self._cursor_value) if self._cursor_value else latest_cursor
            if new_state != self._cursor_value:
                self.logger.info(f"Advancing bookmark for {self.name} stream from {self._cursor_value} to {new_state}")
                self._cursor_value = new_state

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field, self._cursor_value)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(*args, **kwargs)
        latest_cursor = None
        for record in records:
            cursor = record[self.cursor_field]
            latest_cursor = max(cursor, latest_cursor) if latest_cursor else cursor
            yield record
        self._update_state(latest_cursor=latest_cursor)


class Responses(IncrementalRefinerStream):
    cursor_field = "last_shown_at"
    primary_key = "uuid"
    name = "responses"
    state_checkpoint_interval = 10000


# Source
class SourceRefiner(AbstractSource):
    def _getAuth(self, config):
        return TokenAuthenticator(token=config["secret_api_token"], auth_method="Bearer")

    def check_connection(self, logger: AirbyteLogger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            auth = self._getAuth(config)
            Responses(authenticator=auth)
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = auth = self._getAuth(config)
        return [Responses(authenticator=auth)]
