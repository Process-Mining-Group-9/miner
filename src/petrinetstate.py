import jsonpickle
from typing import Set


class StatePlace(object):
    def __init__(self, id: str):
        self.id = id

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, StatePlace):
            return self.id == other.id and self.id == other.id
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(tuple(sorted(self.__dict__.items())))


class StateTransition(object):
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, StateTransition):
            return self.name == other.name
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(self.name)


class StateEdge(object):
    def __init__(self, id: str, source: str, target: str):
        self.id = id
        self.source = source
        self.target = target

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, StateEdge):
            return self.source == other.source and self.target == other.target
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash((self.source, self.target))


class PetriNetState(object):
    def __init__(self, log: str, places: Set[StatePlace], transitions: Set[StateTransition], edges: Set[StateEdge], markings: list):
        self.log = log
        self.places = places
        self.transitions = transitions
        self.edges = edges
        self.markings = markings

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, PetriNetState):
            return self.log == other.log and self.places == other.places and self.transitions == other.transitions and \
                   self.edges == other.edges and self.markings == other.markings
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(tuple(sorted(self.__dict__.items())))

    @classmethod
    def from_json(cls, json_str: str):
        d = jsonpickle.decode(json_str)
        return cls(d['log'], d['places'], d['transactions'], d['edges'], d['markings'])

    def to_json(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)


class Update(object):
    def __init__(self, log: str, new_places: Set[StatePlace], removed_places: Set[StatePlace],
                 new_transitions: Set[StateTransition], removed_transitions: Set[StateTransition],
                 new_edges: Set[StateEdge], removed_edges: Set[StateEdge],
                 markings: list):
        self.log = log
        self.new_places = new_places
        self.new_transitions = new_transitions
        self.new_edges = new_edges
        self.removed_places = removed_places
        self.removed_transitions = removed_transitions
        self.removed_edges = removed_edges
        self.markings = markings

    @classmethod
    def from_json(cls, json_str: str):
        d = jsonpickle.decode(json_str)
        return cls(d['log'], d['new_places'], d['removed_places'], d['new_transactions'], d['removed_transactions'],
                   d['new_edges'], d['removed_edges'], d['markings'])

    def to_json(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)

    def is_not_empty(self) -> bool:
        return self.new_places or self.new_transitions or self.new_edges or self.removed_places or \
               self.removed_transitions or self.removed_edges or self.markings or False  # or False to make expression boolean
