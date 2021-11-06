import jsonpickle


class StatePlace(object):
    def __init__(self, id: str):
        self.id = id


class StateTransition(object):
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name


class StateEdge(object):
    def __init__(self, source: str, target: str):
        self.source = source
        self.target = target


class StateUpdate(object):
    def __init__(self, log: str, places: list[StatePlace], transitions: list[StateTransition], edges: list[StateEdge], markings: list):
        self.log = log
        self.places = places
        self.transitions = transitions
        self.edges = edges
        self.markings = markings

    @classmethod
    def from_json(cls, json_str: str):
        d = jsonpickle.decode(json_str)
        return cls(d['log'], d['places'], d['transactions'], d['edges'], d['markings'])

    def to_json(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
