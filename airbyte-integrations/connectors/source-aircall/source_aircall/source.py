#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from basicauth import encode

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
class AircallStream(HttpStream, ABC):
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
    `class AircallStream(HttpStream, ABC)` which is the current class
    `class Customers(AircallStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(AircallStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalAircallStream((AircallStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://api.aircall.io/v1/"
    time_window = {"months": 1}

    def _get_end_date(self, current_date: pendulum.DateTime, end_date: pendulum.DateTime = pendulum.now()):
        if current_date.add(**self.time_window).date() < end_date.date():
            end_date = current_date.add(**self.time_window)
        return end_date

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        need_new_request = response.json()["meta"]["count"] != response.json()["meta"]["per_page"]
        if need_new_request:
            return None
        return {"page": response.json()["meta"]["current_page"] + 1}

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """This method is called if we run into the rate limit.
        Slack puts the retry time in the `Retry-After` response header so we
        we return that value. If the response is anything other than a 429 (e.g: 5XX)
        fall back on default retry behavior.
        Rate Limits Docs: https://api.slack.com/docs/rate-limits#web"""

        if "X-AircallApi-Reset" in response.headers:
            return int(response.headers["X-AircallApi-Reset"]) - pendulum.now().int_timestamp
        else:
            self.logger.info("X-AircallApi-Reset header not found. Using default backoff value")
            return 5

    def path(self, **kwargs) -> str:
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
        params = {"per_page": 50}
        if stream_slice:
            params.update(stream_slice)
        else:
            params.update({"from": self.start_date.int_timestamp, "to": pendulum.now().int_timestamp})
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
        if response.json()["meta"]["current_page"] == 1:
            self.logger.info(f"{response.json()['meta']['total']} {self.name} to sync in this window (Will fail if > 10000)")
        yield from response.json()[self.name]


class Company(AircallStream):
    primary_key = "id"
    name = "company"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}


# Basic incremental stream
class IncrementalAircallStream(AircallStream, IncrementalMixin):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    def __init__(self, start_date: str, time_window: Mapping[str, int], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._cursor_value = pendulum.parse(start_date).int_timestamp
        self.time_window = time_window

    @property
    @abstractmethod
    def cursor_field(self) -> str:
        pass
    
    @property
    def state(self) -> MutableMapping[str, Any]:
        self.logger.info(f"Getting state: {self._cursor_value}")
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self.logger.info(f"Setting state: {value[self.cursor_field]}")
        self._cursor_value = value[self.cursor_field]

class Calls(IncrementalAircallStream):
    cursor_field = "started_at"
    primary_key = "id"
    name = "calls"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        slices = []
        start = (
            pendulum.from_timestamp(stream_state.get(self.cursor_field)) - pendulum.duration(hours=2) if stream_state else pendulum.from_timestamp(self._cursor_value)
        )
        end = pendulum.now()
        while start < end:
            next = self._get_end_date(start, pendulum.now())
            slice = {"from": start.int_timestamp, "to": next.int_timestamp}
            start = next
            self.logger.info(
                f"Getting data for {pendulum.from_timestamp(slice['from']).to_iso8601_string()} to {pendulum.from_timestamp(slice['to']).to_iso8601_string()}"
            )
            slices.append(slice)

        return slices


# Source
class SourceAircall(AbstractSource):
    def _getAuth(self, config):
        encoded = encode(config["api_id"], config["api_token"]).removeprefix("Basic ")
        return TokenAuthenticator(token=encoded, auth_method="Basic")

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
            Company(authenticator=auth)
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = auth = self._getAuth(config)
        return [Calls(authenticator=auth, start_date=config["start_date"], time_window=config["time_window"])]
