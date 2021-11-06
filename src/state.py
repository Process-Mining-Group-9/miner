import jsonpickle


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
            return self.id == other.id and self.id == other.id
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(tuple(sorted(self.__dict__.items())))


class StateEdge(object):
    def __init__(self, source: str, target: str):
        self.source = source
        self.target = target

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, StateEdge):
            return self.source == other.source and self.target == other.target
        return NotImplemented

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(tuple(sorted(self.__dict__.items())))


class State(object):
    def __init__(self, log: str, places: set[StatePlace], transitions: set[StateTransition], edges: set[StateEdge], markings: list):
        self.log = log
        self.places = places
        self.transitions = transitions
        self.edges = edges
        self.markings = markings

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, State):
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


class StateUpdate(object):
    def __init__(self, log: str, new_places: set[StatePlace], removed_places: set[StatePlace],
                 new_transitions: set[StateTransition], removed_transitions: set[StateTransition],
                 new_edges: set[StateEdge], removed_edges: set[StateEdge],
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
