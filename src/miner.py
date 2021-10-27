from mqtt_event import MqttEvent
import httpx
import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter


class Miner:
    def mine_model(self):
        log = pd.DataFrame.from_records([e.to_min_dict() for e in self.events])
        log = log.sort_values('timestamp')
        parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'process'}
        self.event_log = log_converter.apply(log, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

    def start(self):
        # Start mining the events
        self.mine_model()

    def new_event(self, event: MqttEvent):
        self.events.append(event)
        httpx.post(self.config['db']['address'] + '/events/add', json=event.to_dict())
        self.mine_model()  # Update the event log. Just a prototype atm where it re-mines the log on every new event

    def __init__(self, log: str, config: dict, events: list[MqttEvent] = None):
        self.log_name = log
        self.config = config
        self.events: list[MqttEvent] = events if events is not None else []
        self.event_log = None
