from mqtt_event import MqttEvent
import httpx
import pandas as pd
import os

from pm4py import format_dataframe
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def graphviz(net, initial, final):
    gviz = pn_visualizer.apply(net, initial, final)
    pn_visualizer.view(gviz)


def export_plnm(net, initial, final, file):
    os.makedirs(os.path.dirname(file), exist_ok=True)
    pnml_exporter.apply(net, initial, file, final_marking=final)


class Miner:
    def alpha_miner(self):
        net, initial_marking, final_marking = alpha_miner.apply(self.event_log)
        # graphviz(net, initial_marking, final_marking)
        # export_plnm(net, initial_marking, final_marking, f'../plnm/{self.log_name}.plnm')

    def to_pm4py_log(self):
        log = pd.DataFrame.from_records([e.to_min_dict() for e in self.events])
        log = log.sort_values('timestamp')
        log = format_dataframe(log, case_id='process', activity_key='activity', timestamp_key='timestamp')
        self.event_log = log_converter.apply(log, variant=log_converter.Variants.TO_EVENT_LOG)

    def start(self):
        self.to_pm4py_log()
        self.alpha_miner()

    def new_event(self, event: MqttEvent):
        self.events.append(event)
        httpx.post(self.config['db']['address'] + '/events/add', json=event.to_dict())
        self.to_pm4py_log()  # Update the event log. Just a prototype atm where it re-mines the log on every new event
        self.alpha_miner()

    def __init__(self, log: str, config: dict, events: list[MqttEvent] = None):
        self.log_name = log
        self.config = config
        self.events: list[MqttEvent] = events if events is not None else []
        self.event_log = None
