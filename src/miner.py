from mqtt_event import MqttEvent
import httpx
import json


class Miner:
    def start(self):
        # Start mining the events
        a = 0

    def new_event(self, event: MqttEvent):
        self.events.append(event)
        result = httpx.post(self.config['db']['address'] + '/events/add', json=event.to_dict())
        # Update model with new event

    def __init__(self, log: str, config: dict, events: list[MqttEvent] = None):
        self.log_name = log
        self.config = config
        self.events: list[MqttEvent] = events if events is not None else []
