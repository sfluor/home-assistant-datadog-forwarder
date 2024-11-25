"""
This component forwards metrics to Datadog using the public API.

It requires some configuration to be added to your configuration.yaml file.

Configuration example:

datadog_forwarder:
    tags: "a:b,test:foo"
    prefix: "ha.main_home."
    flush_period_sec: 60
    api_key: my_api_key
    app_key: my_app_key

"""

from __future__ import annotations

import time
import logging

from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType

import voluptuous as vol
from typing import Dict, List
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_series import MetricSeries
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.intake_payload_accepted import IntakePayloadAccepted

from collections import namedtuple, defaultdict

from homeassistant.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_PREFIX,
    EVENT_LOGBOOK_ENTRY,
    EVENT_STATE_CHANGED,
    STATE_UNKNOWN,
)
import homeassistant.helpers.config_validation as cv
from homeassistant.core import HomeAssistant
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers import state as state_helper

_LOGGER = logging.getLogger(__name__)

MetricId = namedtuple("MetricId", ["name", "tags", "unit"])
Value = namedtuple("Value", ["id", "timestamp", "value"])


def send_values(dd_conf: Configuration, values: List[Value]) -> IntakePayloadAccepted:
    with ApiClient(dd_conf) as client:
        api = MetricsApi(client)

        by_name: Dict[MetricId, List[MetricPoint]] = defaultdict(list)

        for value in values:
            by_name[value.id].append(
                MetricPoint(timestamp=value.timestamp, value=value.value)
            )

        series: List[MetricSeries] = []

        for id, points in by_name.items():
            points.sort(key=lambda p: p.timestamp)
            serie = MetricSeries(
                metric=id.name,
                points=points,
                tags=[*id.tags],
                unit=id.unit,
                type=MetricIntakeType.GAUGE,
            )
            series.append(serie)

        body = MetricPayload(series)
        return api.submit_metrics(body=body)


# The domain of your component. Should be equal to the name of your component.
DOMAIN = "datadog_forwarder"


CONF_TAGS = "tags"
CONF_PREFIX = "prefix"
CONF_FLUSH_PERIOD_SEC = "flush_period_sec"
CONF_API_KEY = "api_key"
CONF_APP_KEY = "app_key"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_API_KEY): cv.string,
                vol.Required(CONF_APP_KEY): cv.string,
                vol.Optional(CONF_FLUSH_PERIOD_SEC, default=60): int,
                vol.Optional(CONF_PREFIX, default="hass.datadog"): cv.string,
                vol.Optional(CONF_TAGS, default=""): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


def ts() -> int:
    return int(time.time())


class ValueBuffer:
    def __init__(self, conf: Configuration, flush_period_sec: int):
        self._b: List[Value] = []
        self._last_send: int = ts()
        self._flush_period_sec = flush_period_sec
        self._conf = conf

    def buffer_or_send(self, val: Value):
        self._b.append(val)

        now = ts()
        if now - self._last_send > self._flush_period_sec:
            res = send_values(self._conf, self._b)
            self._last_send = now
            if res.errors:
                _LOGGER.error(
                    "An error occurred sending %d points: %s",
                    len(self._b),
                    ",".join(res.errors),
                )
            self._b = []


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    conf = config[DOMAIN]

    dd_conf = Configuration()

    default_tags = conf["tags"].split(",")
    prefix = conf["prefix"]
    flush_period_sec = conf["flush_period_sec"]

    dd_conf.api_key["apiKeyAuth"] = conf["api_key"]
    dd_conf.api_key["appKeyAuth"] = conf["app_key"]

    buffer = ValueBuffer(dd_conf, flush_period_sec)

    # Will listen on new events and potentially buffer metrics to be sent
    # to the Datadog API.
    def state_changed_listener(event):
        state = event.data.get("new_state")

        if state is None or state.state == STATE_UNKNOWN:
            return

        attrs = dict(state.attributes)
        device_class = attrs.get("device_class", "unknown_device")
        state_class = attrs.get("state_class", "unknown_state")
        metric = f"{prefix}.{state.domain}.{device_class}.{state_class}"
        tags = [
            f"domain:{state.domain}",
            f"entity_id:{state.entity_id}",
            f"device_class:{device_class}",
            f"state_class:{state_class}",
        ] + default_tags
        unit = attrs.get("unit_of_measurement", "")

        ts = state.last_updated_timestamp

        for key, value in attrs.items():
            if isinstance(value, (float, int)):
                attribute = f"{metric}.{key.replace(' ', '_')}"
                value = int(value) if isinstance(value, bool) else value

                # We don't set the unit here since we don't know what's the unit of this nested value.
                m_id = MetricId(attribute, tuple(tags), "")
                buffer.buffer_or_send(Value(m_id, ts, value))
                _LOGGER.debug("Sent metric %s: %s (tags: %s)", attribute, value, tags)

        try:
            value = state_helper.state_as_number(state)
        except ValueError:
            _LOGGER.error("Error sending %s: %s (tags: %s)", metric, state.state, tags)
            return

        m_id = MetricId(metric, tuple(tags), unit)
        buffer.buffer_or_send(Value(m_id, ts, value))

        _LOGGER.debug("Sent metric %s: %s (tags: %s)", metric, value, tags)

    hass.bus.listen(EVENT_STATE_CHANGED, state_changed_listener)

    # Return boolean to indicate that initialization was successfully.
    return True
