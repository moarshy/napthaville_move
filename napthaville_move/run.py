import os
import logging
import json
from typing import Dict
from napthaville_move.schemas import InputSchema
from naptha_sdk.task import Task as NapthaTask


logger = logging.getLogger(__name__)

ALL_PERSONAS = [
    "Isabella Rodriguez",
    "Maria Lopez",
    "Klaus Mueller"
]

ENV_FOLDER = f"{os.getenv('BASE_OUTPUT_DIR')}/napthaville/step-3-3/environment"
META_FOLDER = f"{os.getenv('BASE_OUTPUT_DIR')}/napthaville/step-3-3/reverie"
DEFAULT_START_STEP = 10

env_file = f"{ENV_FOLDER}/{DEFAULT_START_STEP}.json"
with open(env_file, 'r') as f:
    env = json.load(f)

meta_file = f"{META_FOLDER}/meta.json"
with open(meta_file, 'r') as f:
    meta = json.load(f)


async def run(inputs: InputSchema, worker_nodes = None, orchestrator_node = None, flow_run = None, cfg: Dict = None):
    logger.info(f"Running with inputs: {inputs}")
    logger.info(f"Worker nodes: {worker_nodes}")
    logger.info(f"Orchestrator node: {orchestrator_node}")

    # Make sure the worker nodes are available and it is at least 2
    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")

    PERSONA_TO_WORKER = {
        "Isabella Rodriguez": worker_nodes[0],
        "Maria Lopez": worker_nodes[1],
        "Klaus Mueller": worker_nodes[0]
    }    

    PERSONA_TILES = {}
    for persona in ALL_PERSONAS:
        persona_env = env[persona]
        x = persona_env['x']
        y = persona_env['y']
        PERSONA_TILES[persona] = (x, y)

    # Get the persona scratch
    PERSONAS_SCRATCH = {}
    for persona in ALL_PERSONAS:
        task = NapthaTask(
            name="get_scratch",
            fn = 'napthaville_module',
            worker_node=PERSONA_TO_WORKER[persona],
            orchestrator_node=orchestrator_node,
            flow_run=flow_run,
        )
        response = await task(
            task="get_scratch",
            task_params={
                'persona_name': persona
            }
        )

        PERSONAS_SCRATCH[persona] = json.loads(response)

    logger.info(f"PERSONAS_SCRATCH: {PERSONAS_SCRATCH}")

    # Move the persona
    response_after_move = {}
    for persona in ALL_PERSONAS:
        task = NapthaTask(
            name="get_move",
            fn = 'napthaville_module',
            worker_node=PERSONA_TO_WORKER[persona],
            orchestrator_node=orchestrator_node,
            flow_run=flow_run,
        )
        response = await task(
            task = 'get_move',
            task_params = {
                'init_persona_name': persona,
                'personas': json.dumps(PERSONAS_SCRATCH),
                'curr_tile': PERSONA_TILES[persona],
                'curr_time': meta['curr_time']
            }
        )

        response_after_move[persona] = json.loads(response)

    logger.info(f"response_after_move: {response_after_move}")  
            
    
