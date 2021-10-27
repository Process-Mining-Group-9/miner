from fastapi import FastAPI, HTTPException, Request
from multiprocessing import Process, Value
from mqtt_event import MqttEvent
from typing import Dict
from miner import Miner
from custom_logging import CustomizeLogger
from pathlib import Path
import logging
import uvicorn
import yaml
import httpx
import json

miners: Dict[str, Miner] = {}
config: Dict = yaml.safe_load(open('../config.yaml'))


def create_app() -> FastAPI:
    fastapi_app = FastAPI(title='Miner', debug=False)
    custom_logger = CustomizeLogger.make_logger(Path('../logging_config.json'))
    fastapi_app.logger = custom_logger
    return fastapi_app


def get_existing_logs() -> list[str]:
    events_result = httpx.get(config['db']['address'] + '/events')
    if events_result.is_success:
        logs = json.loads(events_result.text)
        logging.info(f'Retrieved existing logs from DB: {logs}')
        return logs
    else:
        logging.error(f'Couldn\'t retrieve existing logs from database. Status code: {events_result.status_code}')


def add_miner(name: str, existing_events: list[MqttEvent]) -> Miner:
    logging.info(f'Adding miner instance for log {name}')
    miner = Miner(name, config, existing_events)
    process = Process(target=miner.start)
    process.start()
    miners[name] = miner
    return miner


def get_existing_data():
    for log in get_existing_logs():
        result = httpx.get(config['db']['address'] + f'/events/{log}')
        events: list[MqttEvent] = []
        if result.is_success:
            events = json.loads(result.text, object_hook=lambda d: MqttEvent(**d))
            logging.info(f'Loaded {len(events)} entries from DB for log {log}')
        else:
            logging.error(f'Couldn\'t load entries from DB for log {log}. Status code: {result.status_code}')
        add_miner(log, events)


app: FastAPI = create_app()
get_existing_data()


@app.post("/notify")
async def notify(request: Request, event: MqttEvent):
    if event.source not in miners:
        add_miner(event.source, [])
    miners[event.source].new_event(event)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
