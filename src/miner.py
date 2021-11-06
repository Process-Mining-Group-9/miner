from state import State, StateUpdate, StatePlace, StateTransition, StateEdge
from multiprocessing import Queue
from mqtt_event import MqttEvent
from typing import Optional
import pandas as pd
import logging
import arrow
import os

from pm4py import format_dataframe
from pm4py.objects.conversion.log import converter
from pm4py.objects.petri_net.obj import PetriNet, Marking
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
        self.petri_net: tuple[Optional[PetriNet], Optional[Marking], Optional[Marking]] = (None, None, None)  # Tuple of Petri net, initial marking, and final marking
        # Register live event stream and starting DFG (Directly Follows Graph) discovery
        self.live_event_stream = LiveEventStream()
        self.streaming_dfg = dfg_discovery.apply()
        self.live_event_stream.register(self.streaming_dfg)
        self.live_event_stream.start()
        # Add initial events to live event stream
        self.append_events_to_stream(self.initial_events)

    def append_events_to_stream(self, events: list[MqttEvent]):
        """Append new events to the live event stream"""
        if events:
            logging.debug(f'Appending {len(events)} new events to stream of "{self.log_name}" miner.')
            event_stream = get_pm4py_stream(events)
            for event in event_stream:
                self.live_event_stream.append(event)

    def update(self):
        """Update the Petri net and broadcast any changes to the WebSocket clients"""
        logging.info(f'Updating Petri net model for miner "{self.log_name}".')
        dfg, activities, start_act, end_act = self.streaming_dfg.get()
        net, initial, final = inductive_miner.apply_dfg(dfg, start_act, end_act, activities)

        if self.config['miner']['save_pictures'] == 'true':
            save_petri_net_image(net, initial, final, name=self.log_name)

        self.create_update(self.petri_net, (net, initial, final))

    def create_update(self, previous: tuple[Optional[PetriNet], Optional[Marking], Optional[Marking]], new: tuple[PetriNet, Marking, Marking]):
        """Compare the previous Petri net and instances to the new one, and send updates to the update queue."""
        p_net, p_init, p_final = previous
        n_net, n_init, n_final = new

        new_state = petri_net_to_state_update(self.log_name, n_net)

        if not p_net:
            new_update_state = StateUpdate(new_state.log, new_state.places, set(), new_state.transitions, set(), new_state.edges, set(), [])
            self.update_queue.put(new_update_state)
            self.petri_net = new
            return

        prev_state = petri_net_to_state_update(self.log_name, p_net)
        update_state = get_update_state(prev_state, new_state)

        if update_state.is_not_empty():
            self.update_queue.put(update_state)
        else:
            logging.info(f'No changes detected in model for miner "{self.log_name}".')

        self.petri_net = new

    def latest_complete_update(self) -> StateUpdate:
        """Get an update that contains the entire Petri net model and ongoing instances.
        This is used to send the latest state for newly connected WebSocket clients."""
        dfg, activities, start_act, end_act = self.streaming_dfg.get()
        net, initial, final = inductive_miner.apply_dfg(dfg, start_act, end_act, activities)
        state = petri_net_to_state_update(self.log_name, net)
        return StateUpdate(self.log_name, state.places, set(), state.transitions, set(), state.edges, set(), [])


# State helper methods


def petri_net_to_state_update(name: str, net: PetriNet) -> State:
    """Convert a Petri net to a simple state object."""
    return State(name, places_to_set(net.places), transitions_to_set(net.transitions), arcs_to_set(net.arcs), [])


def get_update_state(old: State, new: State) -> StateUpdate:
    """Compare two states and determine what is new and what needs to be removed."""
    p_new = new.places - old.places
    p_rem = old.places - new.places
    t_new = new.transitions - old.transitions
    t_rem = old.transitions - new.transitions
    e_new = new.edges - old.edges
    e_rem = old.edges - new.edges
    return StateUpdate(new.log, p_new, p_rem, t_new, t_rem, e_new, e_rem, [])


def places_to_set(places: set[PetriNet.Place]) -> set[StatePlace]:
    """Convert a set of places of a Petri net to a simple set."""
    names: set[StatePlace] = set()
    for p in places:
        names.add(StatePlace(p.name))
    return names


def transitions_to_set(transitions: set[PetriNet.Transition]) -> set[StateTransition]:
    """Convert a set of transitions of a Petri net to a simple set."""
    names: set[StateTransition] = set()
    for t in transitions:
        names.add(StateTransition(t.label if t.label else t.name))
    return names


def arcs_to_set(arcs: set[PetriNet.Arc]) -> set[StateEdge]:
    """Convert a set of transitions of a Petri net to a simple set."""
    names: set[StateEdge] = set()
    for a in arcs:
        source, target = '', ''
        if type(a.source) is PetriNet.Place:
            source = a.source.name
        elif type(a.source) is PetriNet.Transition:
            source = a.source.label if a.source.label else a.source.name

        if type(a.target) is PetriNet.Place:
            target = a.target.name
        elif type(a.target) is PetriNet.Transition:
            target = a.target.label if a.target.label else a.target.name

        names.add(StateEdge(source, target))
    return names
