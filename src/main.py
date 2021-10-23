from fastapi import FastAPI, HTTPException, Request
from multiprocessing import Process, Value
from mqtt_event import MqttEvent
from typing import Dict
from miner import Miner
import uvicorn
import yaml


miners: Dict[str, Miner] = {}


def create_app() -> FastAPI:
    fastapi_app = FastAPI(title='Miner', debug=False)
    return fastapi_app


config: Dict = yaml.safe_load(open('../config.yaml'))
app: FastAPI = create_app()


@app.post("/notify")
async def notify(request: Request, event: MqttEvent):
    if event.source not in miners:
        add_miner(event.source)
    miners[event.source].new_event(event)


def add_miner(name: str) -> Miner:
    miner = Miner()
    process = Process(target=miner.start)
    process.start()
    miners[name] = miner
    return miner


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
