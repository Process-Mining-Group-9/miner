from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi_utils.tasks import repeat_every
from ws_connection_manager import ConnectionManager
from custom_logging import CustomizeLogger
from multiprocessing import Process, Queue
from mqtt_event import MqttEvent
from state import StateUpdate
from typing import Dict
from miner import Miner
import db_helper
import logging
import uvicorn
import yaml

miners: Dict[str, Miner] = {}
update_queues: Dict[str, Queue] = {}
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
    update_queue = Queue()
    miner = Miner(name, config, update_queue, existing_events)
    process = Process(target=miner.start)
    process.start()
    miners[name] = miner
    update_queues[name] = update_queue
    return miner


def discover_existing_data():
    """Query the database for existing event logs, get all of its data, and create a miner process for each event log."""
    for log in db_helper.get_existing_event_logs(config['db']['address']):
        events = db_helper.get_existing_events_of_event_log(config['db']['address'], log)
        add_miner(log, events)


app: FastAPI = create_app()
manager = ConnectionManager()
discover_existing_data()


# REST API Part

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


# WebSockets Part

@app.on_event('startup')
@repeat_every(seconds=1, wait_first=True, raise_exceptions=True)
async def broadcast_queued_updates():
    for log, queue in update_queues.items():
        updates: list[StateUpdate] = []
        while not queue.empty():
            updates.append(queue.get(block=True, timeout=1))
        if updates:
            try:
                for update in updates:
                    update_text = update.to_json()
                    recipients = await manager.broadcast(update_text, log)
                    logging.info(f'Broadcasted update to {recipients} "{log}" clients: {update_text}')
            except Exception as e:
                logging.error(e)


@app.websocket('/ws/{log}')
async def ws(websocket: WebSocket, log: str):
    await manager.connect(websocket, log)
    try:
        logging.info(f'WS connection opened with client from: {websocket.client.host}:{websocket.client.port}')
        if log not in miners.keys():
            logging.warning(f'WS connection opened for log "{log}", but no miner exists for this log.')
            raise WebSocketDisconnect(code=1003)  # https://datatracker.ietf.org/doc/html/rfc6455#section-7.4.1

        # Send latest complete model and ongoing instances
        update = miners[log].latest_complete_update()
        update_text = update.to_json()
        await websocket.send_text(update_text)
        logging.info(f'Sent latest state to newly connected WS client from {websocket.client.host}:{websocket.client.port}: {update_text}')

        while True:  # We need to await something, otherwise the connection will terminate after executing this method
            msg = await websocket.receive_text()
            if msg == 'stop':
                logging.info(f'Received stop command, closing WS connection')
                raise WebSocketDisconnect(code=1000)
    except WebSocketDisconnect as e:
        manager.disconnect(websocket)
        logging.info(f'WS connection closed with client from: {websocket.client.host}:{websocket.client.port}. Status code: {e.code}')


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
