from mqtt_event import MqttEvent


class Miner:
    def start(self):
        print('Starting to mine')

    def new_event(self, event: MqttEvent):
        print(f'Received new event to mine: {event}')

    def __init__(self):
        print('Miner created')
