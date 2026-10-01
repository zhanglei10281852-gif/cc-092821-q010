from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class NotificationGateway(Protocol):
    """通知投递网关：实现方必须按 delivery_key 幂等去重。

    返回 "delivered" 表示本次实际发送，"duplicate" 表示该 delivery_key
    已经投递过、本次被抑制。发布端在确认前崩溃后会用相同的 delivery_key
    重试，网关的幂等保证通知不会重复发出。
    """

    def send(self, *, delivery_key: str, event_type: str, channel: str, payload: dict[str, Any]) -> str:
        ...


@dataclass
class RecordingGateway:
    """进程内记录型网关，模拟带幂等键的通知渠道，供运维演示与确定性测试。"""

    sent: list[dict[str, Any]] = field(default_factory=list)
    _seen: set[str] = field(default_factory=set)

    def send(self, *, delivery_key: str, event_type: str, channel: str, payload: dict[str, Any]) -> str:
        if delivery_key in self._seen:
            return "duplicate"
        self._seen.add(delivery_key)
        self.sent.append({
            "delivery_key": delivery_key,
            "event_type": event_type,
            "channel": channel,
            "payload": payload,
        })
        return "delivered"

    def reset(self) -> None:
        self.sent.clear()
        self._seen.clear()


_default_gateway = RecordingGateway()


def default_gateway() -> RecordingGateway:
    return _default_gateway
