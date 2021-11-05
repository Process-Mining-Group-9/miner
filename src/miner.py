from multiprocessing import Queue
from mqtt_event import MqttEvent
from state import StateUpdate
import pandas as pd
import db_helper
import logging
import os

from pm4py import format_dataframe
from pm4py.objects.conversion.log import converter
from pm4py.streaming.stream.live_event_stream import LiveEventStream
from pm4py.streaming.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def visualize_petri_net(net, initial, final):
    """Visualize a Petri net using graphviz (opens in local image viewer)."""
    gviz = pn_visualizer.apply(net, initial, final)
    pn_visualizer.view(gviz)


def export_to_plnm(net, initial, final, file: str):
    """Export a Petri net to a local PNML file."""
    os.makedirs(os.path.dirname(file), exist_ok=True)
    pnml_exporter.apply(net, initial, file, final_marking=final)


def get_pm4py_stream(events: list[MqttEvent]):
    """Convert a list of event to a Pandas DataFrame compatible with pm4py."""
    # TODO: The streaming discovery doesn't like this format apparently. It doesn't add any events to the DFG.
    log = pd.DataFrame.from_records([e.to_min_dict() for e in events])
    log = log.sort_values(by='timestamp')
    log = format_dataframe(log, case_id='process', activity_key='activity', timestamp_key='timestamp')
    return converter.apply(log, variant=converter.Variants.TO_EVENT_STREAM)


class Miner:
    def __init__(self, log: str, config: dict, update_queue: Queue, events: list[MqttEvent] = None):
        """Initialize the miner with potentially existing events."""
        self.log_name = log
        self.config = config
        self.update_queue = update_queue
        self.initial_events: list[MqttEvent] = events if events is not None else []
        # Register live event stream and starting DFG (Directly Follows Graph) discovery
        self.live_event_stream = LiveEventStream()
        self.streaming_dfg = dfg_discovery.apply()
        self.live_event_stream.register(self.streaming_dfg)
        self.live_event_stream.start()

    def start(self):
        """Start the mining process by discovering the Petri net."""
        logging.info(f'"{self.log_name}" miner: Mining event log to discover Petri net.')
        if self.initial_events:
            event_stream = get_pm4py_stream(self.initial_events)
            for event in event_stream:
                self.live_event_stream.append(event)
        self.update_petri_net()

    def update_petri_net(self):
        # self.live_event_stream.stop()  # somehow never returns. seems to be always blocked.
        dfg, act, sa, ea = self.streaming_dfg.get()
        net, initial, final = inductive_miner.apply_dfg(dfg, act, sa, ea)
        self.live_event_stream.start()

    def add_event(self, event: MqttEvent):
        """Append a new event to the event log and detect changes in derived Petri net."""
        db_helper.add_event(self.config['db']['address'], event)  # Insert new event into database
        self.update_petri_net()
        self.create_update()

    def latest_complete_update(self) -> StateUpdate:
        """Get an update that contains the entire Petri net model and ongoing instances.
        This is used to send the latest state for newly connected WebSocket clients."""
        # TODO: Implement
        update = StateUpdate(self.log_name, [], [], [], [])
        return update

    def create_update(self):
        """Compare the previous Petri net and instances to the new one, and send updates to the update queue."""
        # TODO: Tests with broadcasting updates to WS clients
        update = StateUpdate(self.log_name, [], [], [], [])
        self.update_queue.put(update, block=True, timeout=1)
