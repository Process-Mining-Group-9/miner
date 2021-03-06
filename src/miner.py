from petri_net_state import PetriNetState, Update, StatePlace, StateTransition, StateEdge
from typing import Optional, List, Tuple, Set, Union
from multiprocessing import Queue
from mqtt_event import MqttEvent
import pandas as pd
import logging
import arrow
import uuid
import os

from pm4py import format_dataframe
from pm4py.objects.conversion.log import converter
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.streaming.stream.live_event_stream import LiveEventStream
from pm4py.streaming.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.petri_net.exporter import exporter as pnml_exporter
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def save_petri_net_image(net, initial, final, name: str):
    """Visualize a Petri net using graphviz (opens in local image viewer)."""
    directory = f'../pn_images/{name}'
    os.makedirs(directory, exist_ok=True)
    file = f'{directory}/{arrow.utcnow().int_timestamp}.svg'
    logging.info(f'Saving Petri net image to file: {file}')
    parameters = {pn_visualizer.Variants.WO_DECORATION.value.Parameters.FORMAT: "svg"}
    gviz = pn_visualizer.apply(net, initial, final, parameters=parameters)
    pn_visualizer.save(gviz, file)


def export_to_plnm(net, initial, final, file: str):
    """Export a Petri net to a local PNML file."""
    os.makedirs(os.path.dirname(file), exist_ok=True)
    pnml_exporter.apply(net, initial, file, final_marking=final)


def get_pm4py_stream(events: List[MqttEvent]):
    """Convert a list of event to a Pandas DataFrame compatible with pm4py."""
    if not events:
        return None

    log = pd.DataFrame.from_records([e.to_min_dict() for e in events])
    log = log.sort_values(by='timestamp')
    log = format_dataframe(log, case_id='process', activity_key='activity', timestamp_key='timestamp')
    return converter.apply(log, variant=converter.Variants.TO_EVENT_STREAM)


class Miner:
    def __init__(self, log: str, update_queue: Queue, events: List[MqttEvent] = None):
        """Initialize the miner with potentially existing events."""
        self.log_name = log
        self.update_queue = update_queue
        self.initial_events: List[MqttEvent] = events if events is not None else []
        self.petri_net_state: Optional[PetriNetState] = None

        # Register live event stream and starting DFG (Directly Follows Graph) discovery
        self.live_event_stream = LiveEventStream()
        self.recorded = 0
        self.streaming_dfg = dfg_discovery.apply()
        self.live_event_stream.register(self.streaming_dfg)
        self.live_event_stream.start()

        # Additional feature for performing conformance checking on the model using an existing XES file
        self.do_conformance_check = os.environ['CONFORMANCE_CHECK'] == 'True'
        if self.do_conformance_check:
            os.makedirs('../xes-files', exist_ok=True)
            os.makedirs('../conf-check', exist_ok=True)
            self.xes = xes_importer.apply(f'../xes-files/{self.log_name}.xes', variant=xes_importer.Variants.ITERPARSE, parameters={xes_importer.Variants.ITERPARSE.value.Parameters.TIMESTAMP_SORT: True})
            self.xes_conf_file = open(f'../conf-check/{self.log_name}.csv', 'w')
            self.xes_conf_file.write('Events,Fitness\n')

        # Add initial events to live event stream
        self.append_events_to_stream(self.initial_events)

    def append_events_to_stream(self, events: List[MqttEvent]):
        """Append new events to the live event stream"""
        if events:
            logging.debug(f'Appending {len(events)} new events to stream of "{self.log_name}" miner.')
            event_stream = get_pm4py_stream(events)
            for event in event_stream:
                self.live_event_stream.append(event)
                self.recorded += 1

    def get_petri_net(self) -> Tuple[PetriNet, Marking, Marking]:
        """Get the current Petri net from the event stream."""
        dfg, activities, start_act, end_act = self.streaming_dfg.get()
        return inductive_miner.apply_dfg(dfg, start_act, end_act, activities, variant=inductive_miner.Variants.IMd)

    def conformance_check_xes(self, net, initial, final):
        replay = token_replay.apply(self.xes, net, initial, final)
        avg_fitness = sum([r['trace_fitness'] for r in replay]) / len(replay)
        self.xes_conf_file.write(f'{self.recorded};{avg_fitness}\n')
        self.xes_conf_file.flush()

    def conformance_check(self, events: List[str]):
        """Perform a performance check on the current Petri net with the specified trace."""
        net, initial, final = self.get_petri_net()
        df = pd.DataFrame.from_records([{'process': 'p1', 'activity': e, 'timestamp': i} for i, e in enumerate(events)])
        df = format_dataframe(df, case_id='process', activity_key='activity', timestamp_key='timestamp')
        log = converter.apply(df, variant=converter.Variants.TO_EVENT_LOG)
        replayed_traces = token_replay.apply(log, net, initial, final)
        return replayed_traces

    def name_to_id(self, name: id) -> str:
        """Finds a place or transition with the specified name in the current Petri net, and returns its ID.
           If no match can be found, the name is returned."""
        for p in self.petri_net_state.places:
            if p.name == name:
                return p.id

        for t in self.petri_net_state.transitions:
            if t.name == name:
                return t.id

        return name

    def update(self):
        """Update the Petri net and broadcast any changes to the WebSocket clients"""
        net, initial, final = self.get_petri_net()

        if os.environ['SAVE_PICTURES'] == 'True':
            save_petri_net_image(net, initial, final, name=self.log_name)

        self.create_update(self.petri_net_state, (net, initial, final))

    def create_update(self, prev_state: Optional[PetriNetState], new_petri_net: Tuple[PetriNet, Marking, Marking]):
        """Compare the previous Petri net and instances to the new one, and send updates to the update queue."""
        n_net, n_init, n_final = new_petri_net

        new_state = get_petri_net_state(self.log_name, n_net)

        if not prev_state:
            new_update_state = Update(new_state.id, new_state.places, set(), new_state.transitions, set(), new_state.edges, set())
            self.update_queue.put(new_update_state)
            self.petri_net_state = new_state
            return

        update_state = get_update(prev_state, new_state)

        if update_state.is_not_empty():
            self.update_queue.put(update_state)
            self.update_internal_state(prev_state, new_state)

            if self.do_conformance_check:
                self.conformance_check_xes(n_net, n_init, n_final)

    def update_internal_state(self, old: PetriNetState, new: PetriNetState) -> None:
        """Update the internal state, keeping original ID's of transitions and edges intact."""
        self.petri_net_state.places = new.places

        for new_p in new.places:
            matching = next((p for p in old.places if p == new_p), None)
            if matching:
                new_p.id = matching.id
        self.petri_net_state.places = new.places

        for new_t in new.transitions:
            matching = next((t for t in old.transitions if t == new_t), None)
            if matching:
                new_t.id = matching.id
        self.petri_net_state.transitions = new.transitions

        for new_e in new.edges:
            matching = next((e for e in old.edges if e == new_e), None)
            if matching:
                new_e.id = matching.id
        self.petri_net_state.edges = new.edges

    def latest_complete_update(self) -> Update:
        """Get an update that contains the entire Petri net model and ongoing instances.
        This is used to send the latest state for newly connected WebSocket clients."""
        return Update(self.log_name, self.petri_net_state.places, set(), self.petri_net_state.transitions, set(),
                      self.petri_net_state.edges, set())


# State helper methods


def get_petri_net_state(name: str, net: PetriNet) -> PetriNetState:
    """Convert a Petri net to a simple state object."""
    return PetriNetState(name, places_to_set(net.places), transitions_to_set(net.transitions), arcs_to_set(net.arcs))


def get_update(old: PetriNetState, new: PetriNetState) -> Update:
    """Compare two states and determine what is new and what needs to be removed."""
    p_new = new.places - old.places
    p_rem = old.places - new.places
    t_new = new.transitions - old.transitions
    t_rem = old.transitions - new.transitions
    e_new = new.edges - old.edges
    e_rem = old.edges - new.edges
    return Update(new.id, p_new, p_rem, t_new, t_rem, e_new, e_rem)


def places_to_set(places: Set[PetriNet.Place]) -> Set[StatePlace]:
    """Convert a set of places of a Petri net to a simple set."""
    names: Set[StatePlace] = set()
    for p in places:
        names.add(StatePlace(str(uuid.uuid4()), p.name))
    return names


def transitions_to_set(transitions: Set[PetriNet.Transition]) -> Set[StateTransition]:
    """Convert a set of transitions of a Petri net to a simple set."""
    names: Set[StateTransition] = set()
    for t in transitions:
        names.add(StateTransition(str(uuid.uuid4()), t.label if t.label else t.name))
    return names


def arcs_to_set(arcs: Set[PetriNet.Arc]) -> Set[StateEdge]:
    """Convert a set of transitions of a Petri net to a simple set."""
    names: Set[StateEdge] = set()
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

        names.add(StateEdge(str(uuid.uuid4()), source, target))
    return names
