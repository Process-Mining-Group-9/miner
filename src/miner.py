from multiprocessing import Queue
from mqtt_event import MqttEvent
from state import StateUpdate
import pandas as pd
import logging
import arrow
import os

from pm4py import format_dataframe
from pm4py.objects.conversion.log import converter
from pm4py.streaming.stream.live_event_stream import LiveEventStream
from pm4py.streaming.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def save_petri_net_image(net, initial, final, name: str):
    """Visualize a Petri net using graphviz (opens in local image viewer)."""
    directory = '../pn_images'
    os.makedirs(directory, exist_ok=True)
    file = f'{directory}/{name}_{arrow.utcnow().int_timestamp}.svg'
    logging.info(f'Saving Petri net image to file: {file}')
    parameters = {pn_visualizer.Variants.WO_DECORATION.value.Parameters.FORMAT: "svg"}
    gviz = pn_visualizer.apply(net, initial, final, parameters=parameters)
    pn_visualizer.save(gviz, file)


def export_to_plnm(net, initial, final, file: str):
    """Export a Petri net to a local PNML file."""
    os.makedirs(os.path.dirname(file), exist_ok=True)
    pnml_exporter.apply(net, initial, file, final_marking=final)


def get_pm4py_stream(events: list[MqttEvent]):
    """Convert a list of event to a Pandas DataFrame compatible with pm4py."""
    if not events:
        return None

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
        # Add initial events to live event stream
        self.append_events_to_stream(self.initial_events)

    def append_events_to_stream(self, events: list[MqttEvent]):
        if events:
            logging.debug(f'Appending {len(events)} new events to stream of "{self.log_name}" miner.')
            event_stream = get_pm4py_stream(events)
            for event in event_stream:
                self.live_event_stream.append(event)

    def update(self):
        logging.info(f'Updating Petri net model for miner "{self.log_name}"')
        dfg, activities, start_act, end_act = self.streaming_dfg.get()
        net, initial, final = inductive_miner.apply_dfg(dfg, start_act, end_act, activities)
        # save_petri_net_image(net, initial, final, name=self.log_name)
        self.create_update(net, initial, final)

    def latest_complete_update(self) -> StateUpdate:
        """Get an update that contains the entire Petri net model and ongoing instances.
        This is used to send the latest state for newly connected WebSocket clients."""
        # TODO: Implement
        update = StateUpdate(self.log_name, [], [], [], [])
        return update

    def create_update(self, net, initial, final):
        """Compare the previous Petri net and instances to the new one, and send updates to the update queue."""
        # TODO: Tests with broadcasting updates to WS clients
        update = StateUpdate(self.log_name, [], [], [], [])
        self.update_queue.put(update, block=True, timeout=1)
