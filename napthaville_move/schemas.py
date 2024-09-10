from pydantic import BaseModel
from typing import Optional


class InputSchema(BaseModel):
    num_steps: int = 2
    sims_ipfs_hash: Optional[str] = None