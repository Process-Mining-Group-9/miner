from typing import List

from mqtt_event import MqttEvent
import logging
import httpx
import json
import os


def get_existing_event_logs(db_address: str) -> List[str]:
    try:
        events_result = httpx.get(db_address + '/events')
        if events_result.is_success:
            logs = json.loads(events_result.text)
            logging.info(f'Existing event logs in database: {logs}')
            return logs
        else:
            raise Exception(f'Couldn\'t retrieve existing logs from DB. Status: {events_result}')
    except Exception as e:
        logging.error(e)
        return []


def get_existing_events_of_event_log(db_address: str, event_log: str) -> List[MqttEvent]:
    try:
        result = httpx.get(db_address + f'/events/{event_log}')
        if result.is_success:
            events: List[MqttEvent] = json.loads(result.text, object_hook=lambda d: MqttEvent(**d))
            logging.info(f'Loaded {len(events)} entries from DB for event log {event_log}')
            return events
        else:
            raise Exception(f'Couldn\'t load entries from DB for log {event_log}. Status: {result}')
    except Exception as e:
        logging.error(e)
        return []


def add_event(db_address: str, event: MqttEvent):
    try:
        result = httpx.post(db_address + '/events/add', json=event.to_dict(), headers={'X-Secret': os.environ['SECRET']})
        if not result.is_success:
            raise Exception(f'Couldn\t add new event to DB. Status: {result}')
    except Exception as e:
        logging.error(e)
