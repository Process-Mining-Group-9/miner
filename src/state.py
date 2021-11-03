from pm4py.objects.petri_net.obj import PetriNet


class State:
    def __init__(self):
        self.petri_net = PetriNet()
        self.markings = []
