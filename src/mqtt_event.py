from typing import Optional
from pydantic import BaseModel


class MqttEvent(BaseModel):
    base: str
    source: str
    process: str
    activity: str
    payload: Optional[str] = None

    def to_dict(self) -> dict:
        return {'base': self.base, 'source': self.source, 'process': self.process,
                'activity': self.activity, 'payload': self.payload}

    def __str__(self) -> str:
        return f'{self.base}/{self.source}/{self.process}/{self.activity}: {self.payload}'
