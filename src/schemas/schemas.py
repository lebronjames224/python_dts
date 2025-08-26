from pydantic import BaseModel
from typing import Optional

class JobCreate(BaseModel):
    name: str
    payload: Optional[str] = None
    frequency: str = "once" 
    cron: Optional[str] = None
    next_run_time: Optional[int] = None  
    max_retries: int = 3

class JobOut(BaseModel):
    id: int
    name: str
    status: str
    class Config:
        from_attributes = True