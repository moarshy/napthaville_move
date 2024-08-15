import os
import logging
import json
import uuid
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Dict
from napthaville_move.schemas import InputSchema
from napthaville_move.maze import Maze
from naptha_sdk.task import Task as NapthaTask


logger = logging.getLogger(__name__)

ALL_PERSONAS = [
    "Isabella Rodriguez",
    "Maria Lopez",
    "Klaus Mueller"
]
BASE_SIMS_FOLDER = f"{os.getenv('BASE_OUTPUT_DIR')}/napthaville/step-3-3"
DEFAULT_START_STEP = 10

# env_file = f"{ENV_FOLDER}/{DEFAULT_START_STEP}.json"
# with open(env_file, 'r') as f:
#     env = json.load(f)

# meta_file = f"{META_FOLDER}/meta.json"
# with open(meta_file, 'r') as f:
#     meta = json.load(f)


async def init_simulation_workers(
    worker_nodes = None, 
    orchestrator_node = None, 
    flow_run = None, 
    maze_json = None,
    env = None,
    meta = None
):
    # persona_to_worker
    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")

    persona_to_worker = {
        "Isabella Rodriguez": worker_nodes[0],
        "Maria Lopez": worker_nodes[1],
        "Klaus Mueller": worker_nodes[0]
    }    

    persona_tiles = {}
    for persona in ALL_PERSONAS:
        persona_env = env[persona]
        x = persona_env['x']
        y = persona_env['y']
        persona_tiles[persona] = (x, y)

    sims_folders = {}
    for persona in ALL_PERSONAS:
        task = NapthaTask(
            name="fork_persona",
            fn = 'napthaville_module',
            worker_node=persona_to_worker[persona],
            orchestrator_node=orchestrator_node,
            flow_run=flow_run,
        )
        response = await task(
            task = 'fork_persona',
            task_params = {
                'persona_name': persona,
                'maze_json': maze_json,
                'curr_tile': persona_tiles['persona']
            }
        )
        response = json.loads(response)
        maze_json = response['maze_json']
        sims_folders[persona] = response['sims_folder']

    # start_time, curr_time, sec_per_step
    start_time = meta['start_time']
    curr_time = meta['curr_time']
    sec_per_step = meta['sec_per_step']

    return (
        persona_to_worker,
        sims_folders,
        start_time,
        curr_time,
        sec_per_step,
        persona_tiles,
        maze_json
    )

async def get_scratch(
    persona_to_worker, 
    sims_folders, 
    orchestrator_node, 
    flow_run
):
    personas_scratch = {}
    for persona in ALL_PERSONAS:
        task = NapthaTask(
            name="get_scratch",
            fn = 'napthaville_module',
            worker_node=persona_to_worker[persona],
            orchestrator_node=orchestrator_node,
            flow_run=flow_run,
        )
        response = await task(
            task="get_scratch",
            task_params={
                'persona_name': persona,
                'sims_folder': sims_folders[persona]
            }
        )
        personas_scratch[persona] = json.loads(response)
    return personas_scratch

async def get_single_move(
    persona,
    persona_to_worker, 
    sims_folders,
    personas_scratch,
    persona_tiles,
    curr_time,
    orchestrator_node, 
    flow_run
):
    task = NapthaTask(
        name="get_move",
        fn = 'napthaville_module',
        worker_node=persona_to_worker[persona],
        orchestrator_node=orchestrator_node,
        flow_run=flow_run,
    )
    response = await task(
        task = 'get_move',
        task_params = {
            'init_persona_name': persona,
            'sims_folder': sims_folders[persona],
            'personas': json.dumps(personas_scratch),
            'curr_tile': persona_tiles[persona],
            'curr_time': curr_time
        }
    )
    return json.loads(response)

async def save_a_step(
    step,
    persona_tiles,
    curr_time,
    response_after_move,
    persona_to_worker,
    sims_folders,
    orchestrator_node,
    flow_run
):
    for persona in ALL_PERSONAS:
        task = NapthaTask(
            name="save_step",
            fn='napthaville_module',
            worker_node=persona_to_worker[persona],
            orchestrator_node=orchestrator_node,
            flow_run=flow_run,
        )
        await task(
            task='save_step',
            task_params={
                'persona_name': persona,
                'step': step,
                'curr_tile': persona_tiles[persona],
                'curr_time': curr_time,
                'move_data': json.dumps(response_after_move[persona]),
                'sims_folder': sims_folders[persona]
            }
        )


async def fork_sims_folder():
    new_sims_folder = Path(os.getenv('BASE_OUTPUT_DIR', '')) / str(uuid.uuid4())
    new_sims_folder.mkdir(parents=True, exist_ok=True)
    shutil.copytree(BASE_SIMS_FOLDER, new_sims_folder)
    return new_sims_folder.name


async def run(inputs: InputSchema, worker_nodes = None, orchestrator_node = None, flow_run = None, cfg: Dict = None):
    logger.info(f"Running with inputs: {inputs}")
    logger.info(f"Worker nodes: {worker_nodes}")
    logger.info(f"Orchestrator node: {orchestrator_node}")

    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")
    
    orchestrator_sims_folder = fork_sims_folder()

    env_file = f"{orchestrator_sims_folder}/environment/{DEFAULT_START_STEP}.json"
    with open(env_file, 'r') as f:
        env = json.load(f)

    meta_file = f"{orchestrator_sims_folder}/reverie/meta.json"
    with open(meta_file, 'r') as f:
        meta = json.load(f)

    maze = Maze(maze_name="napthaville")

    (
        persona_to_worker,
        sims_folders,
        start_time,
        curr_time,
        sec_per_step,
        persona_tiles,
        maze_json
    ) = await init_simulation_workers(
        worker_nodes=worker_nodes, 
        orchestrator_node=orchestrator_node, 
        flow_run=flow_run,
        maze_json=maze.to_json(),
        env=env,
        meta=meta
    )
    logger.info("init complete")

    NUM_STEPS = inputs.get('num_steps', 5)
    game_obj_cleanup = dict()
    curr_step = DEFAULT_START_STEP
    maze = Maze.from_json(maze_json)

    for step in range(NUM_STEPS):
        logger.info(f"Starting step {step + 1}")
        curr_env_file = f"{orchestrator_sims_folder}/environment/{curr_step}.json"
        with open(curr_env_file) as json_file:
            new_env = json.load(json_file)

        for k, v in game_obj_cleanup.items():
            maze.turn_event_from_tile_idle(k, v)

        personas_scratch = await get_scratch(
            persona_to_worker, 
            sims_folders, 
            orchestrator_node, 
            flow_run
        )

        game_obj_cleanup = dict()

        for persona in ALL_PERSONAS:
            curr_tile = persona_tiles[persona]
            new_tile = new_env[persona]['x'], new_env[persona]['y']
            persona_tiles[persona] = new_tile
            maze.remove_subject_events_from_tile(persona, curr_tile)

            persona_scratch = personas_scratch[persona]
            act_address = persona_scratch.get('act_address', None)
            if not act_address:
                _curr = ("", None, None, None)
            else:
                _curr = (
                    act_address, 
                    persona_scratch['act_obj_event'][1], 
                    persona_scratch['act_obj_event'][2], 
                    persona_scratch['act_obj_description']
                )

            maze.add_event_from_tile(_curr, new_tile)
            blank = (_curr[0], None, None, None)
            maze.remove_event_from_tile(blank, new_tile)


        movements = {
            'persona': dict(),
            'meta': dict()
        }

        for persona in ALL_PERSONAS:
            response = await get_single_move(
                persona,
                persona_to_worker, 
                sims_folders,
                personas_scratch,
                persona_tiles,
                curr_time,
                orchestrator_node, 
                flow_run
            )
            execute_response = response['execute_response']
            chat_response = response['chat']
            new_tile, pronunciation, description = execute_response

            movements["persona"][persona] = {}
            movements["persona"][persona]["movement"] = new_tile
            movements["persona"][persona]["pronunciation"] = pronunciation
            movements["persona"][persona]["description"] = description
            movements["persona"][persona]["chat"] = chat_response

        logger.info(f"Response after move: {movements}")
        
        movements["meta"]["curr_time"] = (curr_time .strftime("%B %d, %Y, %H:%M:%S"))

        curr_move_file = f"{orchestrator_sims_folder}/movement/{step}.json"
        with open(curr_move_file, "w") as outfile: 
            outfile.write(json.dumps(movements, indent=2))

        curr_time += timedelta(seconds=sec_per_step)
        curr_step += 1

    # save the final maze
    final_maze_file = f"{orchestrator_sims_folder}/maze.json"
    with open(final_maze_file, "w") as outfile: 
        outfile.write(maze.to_json())

    # save folder information
    folder_info = {
        "orchestrator_sims_folder": orchestrator_sims_folder,
        "personas": ALL_PERSONAS,
        "num_steps": NUM_STEPS,
        "start_time": start_time,
        "end_time": curr_time,
        "sec_per_step": sec_per_step,
        "personas_to_worker": persona_to_worker,
        "sims_folders": sims_folders
    }
    with open(f"{orchestrator_sims_folder}/simulation_info.json", "w") as outfile: 
        outfile.write(json.dumps(folder_info, indent=2))