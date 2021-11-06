from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi_utils.tasks import repeat_every
from ws_connection_manager import ConnectionManager
from custom_logging import CustomizeLogger
from multiprocessing import Process, Queue
from mqtt_event import MqttEvent
from state import State
from typing import Dict
from miner import Miner
import db_helper
import logging
import uvicorn
import yaml

miners: Dict[str, Miner] = {}
new_event_queue: Dict[str, Queue] = {}
ws_updates_queue: Dict[str, Queue] = {}
config: Dict = yaml.safe_load(open('../config.yaml'))


def create_app() -> FastAPI:
    """Create a FastAPI instance for this application."""
    fastapi_app = FastAPI(title='Miner', debug=False)
    custom_logger = CustomizeLogger.make_logger(config['log'])
    fastapi_app.logger = custom_logger
    return fastapi_app


def add_event_to_queue(event: MqttEvent, log: str):
    if log not in new_event_queue:
        new_event_queue[log] = Queue()
    new_event_queue[log].put(event)


def discover_existing_data():
    """Query the database for existing event logs, get all of its data, and create a miner process for each event log."""
    for log in db_helper.get_existing_event_logs(config['db']['address']):
        events = db_helper.get_existing_events_of_event_log(config['db']['address'], log)
        for event in events:
            add_event_to_queue(event, log)


app: FastAPI = create_app()
ws_manager = ConnectionManager()
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
    if request.client.host not in ('127.0.0.1', 'localhost'):
        raise HTTPException(status_code=403, detail='Remote access to this endpoint is not allowed.')

    if not event.source:
        raise HTTPException(status_code=400, detail='Source value must be set')

    logging.info(f'Received new event notification: {event}')
    db_helper.add_event(config['db']['address'], event)
    add_event_to_queue(event, event.source)


@app.on_event('startup')
@repeat_every(seconds=5, wait_first=False, raise_exceptions=True)
async def append_new_events():
    """Append new events from the queue to the miner's live event stream."""
    for log, queue in new_event_queue.items():
        events: list[MqttEvent] = []
        while not queue.empty():
            events.append(queue.get())
        if events:
            if log not in miners:
                ws_update_queue = Queue()
                logging.info(f'Creating new miner for "{log}" with {len(events)} initial events.')
                miners[log] = Miner(log, config, ws_update_queue, events)
                ws_updates_queue[log] = ws_update_queue
            else:
                logging.info(f'Appending {len(events)} new event for "{log}".')
                miners[log].append_events_to_stream(events)


@app.on_event('startup')
@repeat_every(seconds=10, wait_first=False, raise_exceptions=True)
async def run_miner_updates():
    """Periodically update the model derived from the live event stream of each miner."""
    for log, miner in miners.items():
        logging.debug(f'Updating model for "{log}" miner.')
        miner.update()


# WebSockets Part


@app.on_event('startup')
@repeat_every(seconds=1, wait_first=False, raise_exceptions=True)
async def broadcast_queued_updates():
    for log, queue in ws_updates_queue.items():
        updates: list[State] = []
        while not queue.empty():
            updates.append(queue.get(block=True, timeout=1))
        if updates:
            try:
                for update in updates:
                    update_text = update.to_json()
                    recipients = await ws_manager.broadcast(update_text, log)
                    logging.info(f'Broadcasted update to {recipients} "{log}" clients: {update_text}')
            except Exception as e:
                logging.error(e)


@app.websocket('/ws/{log}')
async def ws(websocket: WebSocket, log: str):
    await ws_manager.connect(websocket, log)
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
        ws_manager.disconnect(websocket)
        logging.info(f'WS connection closed with client from: {websocket.client.host}:{websocket.client.port}. Status code: {e.code}')


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
