import jsonpickle


class StateUpdate(object):
    def __init__(self, log: str, places: list, transactions: list, edges: list, markings: list):
        self.log = log
        self.places = places
        self.transactions = transactions
        self.edges = edges
        self.markings = markings

    @classmethod
    def from_json(cls, json_str: str):
        d = jsonpickle.decode(json_str)
        return cls(d['log'], d['places'], d['transactions'], d['edges'], d['markings'])

    def to_json(self) -> str:
        return jsonpickle.encode(self, unpicklable=False)
