from mqtt_event import MqttEvent
import logging
import httpx
import json


def get_existing_event_logs(db_address: str) -> list[str]:
    try:
        events_result = httpx.get(db_address + '/events')
        if events_result.is_success:
            logs = json.loads(events_result.text)
            logging.info(f'Existing event logs in database: {logs}')
            return logs
        else:
            raise Exception(f'Couldn\'t retrieve existing logs from database. Status code: {events_result.status_code}')
    except Exception as e:
        logging.error(e)
        return []


def get_existing_events_of_event_log(db_address: str, event_log: str) -> list[MqttEvent]:
    try:
        result = httpx.get(db_address + f'/events/{event_log}')
        if result.is_success:
            events: list[MqttEvent] = json.loads(result.text, object_hook=lambda d: MqttEvent(**d))
            logging.info(f'Loaded {len(events)} entries from DB for event log {event_log}')
            return events
        else:
            raise Exception(f'Couldn\'t load entries from DB for log {event_log}. Status code: {result.status_code}')
    except Exception as e:
        logging.error(e)
        return []
