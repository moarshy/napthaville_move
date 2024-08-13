from pydantic import BaseModel


class InputSchema(BaseModel):
    num_steps: int = 1