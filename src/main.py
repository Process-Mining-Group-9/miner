from fastapi import FastAPI, HTTPException, Request
from multiprocessing import Process, Value
from mqtt_event import MqttEvent
from typing import Dict
from miner import Miner
import uvicorn
import yaml
import httpx
import json


miners: Dict[str, Miner] = {}


def create_app() -> FastAPI:
    fastapi_app = FastAPI(title='Miner', debug=False)
    return fastapi_app


config: Dict = yaml.safe_load(open('../config.yaml'))
app: FastAPI = create_app()


def get_existing_logs() -> list[str]:
    events_result = httpx.get(config['db']['address'] + '/events')
    if events_result.is_success:
        return json.loads(events_result.text)


@app.post("/notify")
async def notify(request: Request, event: MqttEvent):
    if event.source not in miners:
        add_miner(event.source, [])
    miners[event.source].new_event(event)


def add_miner(name: str, existing_events: list[MqttEvent]) -> Miner:
    miner = Miner(name, config, existing_events)
    process = Process(target=miner.start)
    process.start()
    miners[name] = miner
    return miner


if __name__ == '__main__':
    for log in get_existing_logs():
        result = httpx.get(config['db']['address'] + f'/events/{log}')
        events: list[MqttEvent] = []
        if result.is_success:
            events = json.loads(result.text, object_hook=lambda d: MqttEvent(**d))
        add_miner(log, events)
    uvicorn.run(app, host='0.0.0.0', port=8001)
