from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from ws_connection_manager import ConnectionManager
from custom_logging import CustomizeLogger
from multiprocessing import Process
from mqtt_event import MqttEvent
from typing import Dict
from miner import Miner
import db_helper
import logging
import uvicorn
import yaml

miners: Dict[str, Miner] = {}
config: Dict = yaml.safe_load(open('../config.yaml'))


def create_app() -> FastAPI:
    """Create a FastAPI instance for this application."""
    fastapi_app = FastAPI(title='Miner', debug=False)
    custom_logger = CustomizeLogger.make_logger(config['log'])
    fastapi_app.logger = custom_logger
    return fastapi_app


def add_miner(name: str, existing_events: list[MqttEvent]) -> Miner:
    """Add a miner process to the process pool."""
    logging.info(f'Adding miner instance for log {name}')
    miner = Miner(name, config, existing_events)
    process = Process(target=miner.start)
    process.start()
    miners[name] = miner
    return miner


def discover_existing_data():
    """Query the database for existing event logs, get all of its data, and create a miner process for each event log."""
    for log in db_helper.get_existing_event_logs(config['db']['address']):
        events = db_helper.get_existing_events_of_event_log(config['db']['address'], log)
        add_miner(log, events)


app: FastAPI = create_app()
manager = ConnectionManager()
discover_existing_data()


@app.get('/')
async def root():
    """Redirect to the interactive Swagger documentation on root."""
    return RedirectResponse(url='/docs')


@app.get('/logs')
async def logs(request: Request):
    """Gets a list of logs available to connect to via WebSockets"""
    return JSONResponse(list(miners.keys()))


# TODO: Restrict access to only localhost
@app.post('/notify')
async def notify(request: Request, event: MqttEvent):
    """Notify a miner of a new event, and create a new miner if the event log hasn't been encountered yet."""
    if not event.source:
        raise HTTPException(status_code=400, detail=f'Source value must be set')

    logging.info(f'Received new event notification: {event}')
    if event.source not in miners:
        add_miner(event.source, [])
    miners[event.source].add_event(event)


@app.websocket('/ws/{log}')
async def get(websocket: WebSocket, log: str):
    await manager.connect(websocket)
    try:
        logging.info(f'WS connection opened with client from: {websocket.client.host}:{websocket.client.port}')
        if log not in miners.keys():
            logging.warning(f'A WS connection was opened for log "{log}", but no miner exists for this log.')
            raise WebSocketDisconnect(code=1003)  # https://datatracker.ietf.org/doc/html/rfc6455#section-7.4.1

        await manager.send_personal_message(f'Hello, soon we will send some data for {log}', websocket)  # TODO: Placeholder
        while True:
            response = await websocket.receive_text()  # Placeholder to not close connection right after
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logging.info(f'WS connection closed with client from: {websocket.client.host}:{websocket.client.port}')


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
