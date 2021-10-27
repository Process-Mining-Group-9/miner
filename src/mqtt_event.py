from typing import Optional
from pydantic import BaseModel


class MqttEvent(BaseModel):
    rowid: Optional[int] = None
    timestamp: float
    base: Optional[str] = None
    source: Optional[str] = None
    process: str
    activity: str
    payload: Optional[str] = None

    def to_min_dict(self) -> dict:
        return {'timestamp': self.timestamp, 'process': self.process,
                'activity': self.activity, 'payload': self.payload}

    def to_dict(self) -> dict:
        return {'rowid': self.rowid, 'timestamp': self.timestamp, 'base': self.base, 'source': self.source,
                'process': self.process, 'activity': self.activity, 'payload': self.payload}

    def __str__(self) -> str:
        return f'{self.timestamp}: {self.base}/{self.source}/{self.process}/{self.activity}: {self.payload}'
