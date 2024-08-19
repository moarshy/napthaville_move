from pydantic import BaseModel


class InputSchema(BaseModel):
    start_step: int = 10
    num_steps: int = 2