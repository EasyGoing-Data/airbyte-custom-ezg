#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
import datetime
import pendulum
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode
import pandas as pd
import numpy as np


""" Base Stream """


class ApplovinMaxStream(HttpStream, ABC):
    url_base = "https://r.applovin.com/"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}

    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return None


""" Check connection + app discovery """


class ApplovinMaxCheckConnection(ApplovinMaxStream):
    primary_key = None

    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return "maxReport"

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=45)
        return {
            "start": start_date,
            "end": today,
            "api_key": self.config["api_key"],
            "format": "json",
            "columns": "package_name,platform",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json.get("results", [])


""" User-Level Ad Impression Report """


class ApplovinMaxUserLevelAdImpressionReport(ApplovinMaxStream, IncrementalMixin):
    primary_key = None

    def __init__(self, list_app, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = self.config.get("number_days_backward", 7)
        self.timezone = self.config.get("timezone", "UTC")
        self.get_last_X_days = self.config.get("get_last_X_days", False)
        self.list_app = list_app
        self._raise_on_http_errors = True
        self._current_slice = {}

    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return "max/userAdRevenueReport"

    @property
    def name(self) -> str:
        return "User_Level_Ad_Impression_Report"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        # FIX 1: bo .add(days=1) — cong don +1 moi lan load state lam cursor
        # troi dan vao tuong lai (root cause su co 2026-07-02).
        # Lookback da co number_days_backward lo, khong can +1.
        cursor = pendulum.parse(value[self.cursor_field]).date()

        # FIX 2: clamp cursor <= today — tu chua lanh neu state cu da bi
        # ghi ngay tuong lai, khong can reset state thu cong qua API.
        today = pendulum.today(self.timezone).date()
        if cursor > today:
            self.logger.info(f"Cursor {cursor} o tuong lai -> clamp ve {today}")
            cursor = today

        self._cursor_value = cursor
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    def stream_slices(self, stream_state=None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        slice = []
        data_available_date = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            start_date = pendulum.today(self.timezone).subtract(days=self.number_days_backward).date()
        elif stream_state:
            start_date = self.state[self.cursor_field].subtract(days=self.number_days_backward)
        else:
            start_date = pendulum.parse(self.config["start_date"]).date()

        # Chot an toan: API chi cho phep 45 ngay gan nhat
        earliest_allowed = pendulum.today(self.timezone).subtract(days=45).date()
        if start_date < earliest_allowed:
            self.logger.info(f"start_date {start_date} qua 45 ngay -> kep ve {earliest_allowed}")
            start_date = earliest_allowed

        # FIX 3: canh bao khi khong co app nao (list_app rong) — truoc day
        # se sinh slice rong mot cach am tham.
        if not self.list_app:
            self.logger.warning("list_app rong — khong co app nao de sync (check API key / maxReport).")

        while start_date <= data_available_date:
            start_date_as_str = start_date.to_date_string()
            for app in self.list_app:
                if not app.get("package_name"):
                    continue
                slice.append({
                    "date": start_date_as_str,
                    "application": app["package_name"],
                    "platform": app["platform"],
                })
            start_date = start_date.add(days=1)

        # FIX 4: bo "or [None]" — slice rong thi tra ve [] de CDK skip sync
        # sach se, thay vi truyen stream_slice=None gay TypeError.
        if not slice:
            self.logger.info("Khong co slice nao (start_date > today hoac list_app rong) — skip.")
        return slice

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        # FIX 5 (phong thu): guard stream_slice=None de khong bao gio
        # crash 'NoneType' object is not iterable lan nua.
        request_params = dict(stream_slice or {})  # date, application, platform
        request_params.update({"api_key": self.config["api_key"]})
        request_params.update({"aggregated": False})
        self.logger.info(f"stream slice {stream_slice}")
        return request_params

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        self._current_slice = stream_slice or {}
        records = super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        )
        for record in records:
            if record and record.get(self.cursor_field):
                record_cursor_value = pendulum.parse(record[self.cursor_field]).date()
                self._cursor_value = max(self._cursor_value, record_cursor_value) if self._cursor_value else record_cursor_value
            yield record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        if response.status_code == 404:
            return
        response_json = response.json()
        # Uu tien ban co Meta bidding (co cot Network); neu khong co thi dung url thuong
        ad_revenue_report_url = response_json.get("ad_revenue_report_url") or response_json.get("url")
        if not ad_revenue_report_url:
            return
        app_id = self._current_slice.get("application")
        platform = self._current_slice.get("platform")
        for chunk in pd.read_csv(ad_revenue_report_url, chunksize=100000):
            chunk.rename(columns=lambda x: x.replace(" ", "_").lower(), inplace=True)
            chunk.replace([np.nan, np.inf, -np.inf], None, inplace=True)
            for record in chunk.to_dict(orient="records"):
                record["app_id"] = app_id
                record["platform"] = platform
                yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": {
                "app_id": {"type": ["null", "string"]},
                "platform": {"type": ["null", "string"]},
                "date": {"type": ["null", "string"]},
                "ad_format": {"type": ["null", "string"]},
                "ad_placement": {"type": ["null", "string"]},
                "ad_unit_id": {"type": ["null", "string"]},
                "ad_unit_name": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "device_type": {"type": ["null", "string"]},
                "idfa": {"type": ["null", "string"]},
                "idfv": {"type": ["null", "string"]},
                "network": {"type": ["null", "string"]},
                "placement": {"type": ["null", "string"]},
                "revenue": {"type": ["null", "number"]},
                "user_id": {"type": ["null", "string"]},
                "waterfall": {"type": ["null", "string"]},
            },
        }

    @property
    def raise_on_http_errors(self) -> bool:
        return self._raise_on_http_errors

    @raise_on_http_errors.setter
    def raise_on_http_errors(self, value: bool):
        self._raise_on_http_errors = value

    def should_retry(self, response: requests.Response) -> bool:
        if response.status_code == 404:
            setattr(self, "raise_on_http_errors", False)
            return False
        return super().should_retry(response=response)


# Source
class SourceApplovinMax(AbstractSource):
    def _get_list_app(self, config) -> List[Mapping[str, Any]]:
        check_stream = ApplovinMaxCheckConnection(config=config)
        records = check_stream.read_records(sync_mode="full_refresh")
        results = next(records)  # list of dicts: {package_name, platform}
        apps = []
        seen = set()
        for r in results:
            package_name = r.get("package_name")
            platform = r.get("platform")
            if not package_name:
                continue
            key = (package_name, platform)
            if key in seen:
                continue
            seen.add(key)
            apps.append({"package_name": package_name, "platform": platform})
        return apps

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            apps = self._get_list_app(config)
            logger.info(f"Discovered {len(apps)} app(s): {apps}")
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        apps = self._get_list_app(config)
        return [ApplovinMaxUserLevelAdImpressionReport(config=config, list_app=apps)]