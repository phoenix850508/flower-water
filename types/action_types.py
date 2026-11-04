from typing import TypedDict
from collections.abc import Callable, Awaitable
from dataclasses import dataclass, field
from reactivex.subject import Subject
from reactivex import Observable

class Moisture_sensor(TypedDict):
    moisture: float

@dataclass
class Sensor_data(TypedDict):
    type: str
    payload: Moisture_sensor
    metadata: dict = field(default_factory=dict)


action_s: Subject[Action] = Subject()

GetHandler = Callable[[Subject], [Awaitable, None]]
DispatchHandler = Callable[[Observable], [Awaitable, None]]
