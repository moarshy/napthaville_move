import os
import logging
import json
import uuid
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any
from functools import partial
import asyncio

from napthaville_move.schemas import InputSchema
from napthaville_move.maze import Maze
from naptha_sdk.task import Task as NapthaTask

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration from a file (you'd need to create this)
from config import (
    ALL_PERSONAS,
    BASE_SIMS_FOLDER,
    DEFAULT_START_STEP,
    NUM_STEPS
)

class SimulationManager:
    def __init__(self, worker_nodes: List[str], orchestrator_node: str, flow_run: Any):
        self.worker_nodes = worker_nodes
        self.orchestrator_node = orchestrator_node
        self.flow_run = flow_run
        self.persona_to_worker = self._assign_personas_to_workers()
        self.maze = Maze(maze_name="napthaville")
        self.sims_folders: Dict[str, str] = {}
        self.persona_tiles: Dict[str, Tuple[int, int]] = {}
        self.curr_time: datetime
        self.sec_per_step: int

    def _assign_personas_to_workers(self) -> Dict[str, str]:
        """Assign personas to workers in a round-robin fashion."""
        return {persona: self.worker_nodes[i % len(self.worker_nodes)] 
                for i, persona in enumerate(ALL_PERSONAS)}

    async def init_simulation(self):
        """Initialize the simulation environment."""
        self.orchestrator_sims_folder = await self.fork_sims_folder()
        env, meta = self.load_initial_state()
        await self.init_workers(env, meta)
        self.curr_time = datetime.strptime(meta['curr_time'], "%B %d, %Y, %H:%M:%S")
        self.sec_per_step = meta['sec_per_step']

    async def fork_sims_folder(self) -> str:
        """Create a new simulation folder."""
        new_sims_folder = Path(os.getenv('BASE_OUTPUT_DIR', '')) / str(uuid.uuid4())
        new_sims_folder.mkdir(parents=True, exist_ok=True)
        shutil.copytree(BASE_SIMS_FOLDER, new_sims_folder)
        return str(new_sims_folder)

    def load_initial_state(self) -> Tuple[Dict, Dict]:
        """Load initial environment and metadata."""
        with open(f"{self.orchestrator_sims_folder}/environment/{DEFAULT_START_STEP}.json", 'r') as f:
            env = json.load(f)
        with open(f"{self.orchestrator_sims_folder}/reverie/meta.json", 'r') as f:
            meta = json.load(f)
        return env, meta

    async def init_workers(self, env: Dict, meta: Dict):
        """Initialize worker nodes with persona data."""
        tasks = []
        for persona in ALL_PERSONAS:
            task = self.create_task("fork_persona", self.persona_to_worker[persona])
            tasks.append(task(
                persona_name=persona,
                maze_json=self.maze.to_json(),
                curr_tile=(env[persona]['x'], env[persona]['y'])
            ))
        responses = await asyncio.gather(*tasks)
        for persona, response in zip(ALL_PERSONAS, responses):
            response_data = json.loads(response)
            self.sims_folders[persona] = response_data['sims_folder']
            self.persona_tiles[persona] = (env[persona]['x'], env[persona]['y'])

    def create_task(self, name: str, worker: str) -> NapthaTask:
        """Create a NapthaTask with common parameters."""
        return partial(NapthaTask(
            name=name,
            fn='napthaville_module',
            worker_node=worker,
            orchestrator_node=self.orchestrator_node,
            flow_run=self.flow_run
        ))

    async def run_simulation(self, num_steps: int):
        """Run the simulation for a specified number of steps."""
        for step in range(num_steps):
            logger.info(f"Starting step {step + 1}")
            await self.process_step(step)
            self.curr_time += timedelta(seconds=self.sec_per_step)

        await self.save_final_state()

    async def process_step(self, step: int):
        """Process a single simulation step."""
        new_env = self.load_environment(step)
        personas_scratch = await self.get_all_persona_scratch()
        movements = await self.get_all_persona_moves(personas_scratch)
        await self.update_environment(new_env, personas_scratch)
        await self.save_movements(step, movements)

    def load_environment(self, step: int) -> Dict:
        """Load the environment state for the current step."""
        with open(f"{self.orchestrator_sims_folder}/environment/{DEFAULT_START_STEP + step}.json") as json_file:
            return json.load(json_file)

    async def get_all_persona_scratch(self) -> Dict[str, Dict]:
        """Get scratch data for all personas concurrently."""
        tasks = [self.create_task("get_scratch", self.persona_to_worker[persona])(
            persona_name=persona,
            sims_folder=self.sims_folders[persona]
        ) for persona in ALL_PERSONAS]
        responses = await asyncio.gather(*tasks)
        return {persona: json.loads(response) for persona, response in zip(ALL_PERSONAS, responses)}

    async def get_all_persona_moves(self, personas_scratch: Dict[str, Dict]) -> Dict[str, Dict]:
        """Get moves for all personas concurrently."""
        tasks = [self.create_task("get_move", self.persona_to_worker[persona])(
            init_persona_name=persona,
            sims_folder=self.sims_folders[persona],
            personas=json.dumps(personas_scratch),
            curr_tile=self.persona_tiles[persona],
            curr_time=self.curr_time.strftime("%B %d, %Y, %H:%M:%S")
        ) for persona in ALL_PERSONAS]
        responses = await asyncio.gather(*tasks)
        return {persona: json.loads(response) for persona, response in zip(ALL_PERSONAS, responses)}

    async def update_environment(self, new_env: Dict, personas_scratch: Dict[str, Dict]):
        """Update the environment based on persona movements."""
        for persona in ALL_PERSONAS:
            curr_tile = self.persona_tiles[persona]
            new_tile = (new_env[persona]['x'], new_env[persona]['y'])
            self.persona_tiles[persona] = new_tile
            self.maze.remove_subject_events_from_tile(persona, curr_tile)
            self.maze.add_event_from_tile(self.get_persona_event(personas_scratch[persona]), new_tile)

    def get_persona_event(self, persona_scratch: Dict) -> Tuple[str, Any, Any, Any]:
        """Get the current event for a persona."""
        act_address = persona_scratch.get('act_address')
        if not act_address:
            return ("", None, None, None)
        return (
            act_address,
            persona_scratch['act_obj_event'][1],
            persona_scratch['act_obj_event'][2],
            persona_scratch['act_obj_description']
        )

    async def save_movements(self, step: int, movements: Dict[str, Dict]):
        """Save the movements for the current step."""
        movements["meta"] = {"curr_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S")}
        with open(f"{self.orchestrator_sims_folder}/movement/{step}.json", "w") as outfile:
            json.dump(movements, outfile, indent=2)

    async def save_final_state(self):
        """Save the final state of the simulation."""
        with open(f"{self.orchestrator_sims_folder}/maze.json", "w") as outfile:
            json.dump(self.maze.to_json(), outfile, indent=2)
        
        folder_info = {
            "orchestrator_sims_folder": self.orchestrator_sims_folder,
            "personas": ALL_PERSONAS,
            "num_steps": NUM_STEPS,
            "start_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
            "end_time": (self.curr_time + timedelta(seconds=self.sec_per_step * NUM_STEPS)).strftime("%B %d, %Y, %H:%M:%S"),
            "sec_per_step": self.sec_per_step,
            "personas_to_worker": self.persona_to_worker,
            "sims_folders": self.sims_folders
        }
        with open(f"{self.orchestrator_sims_folder}/simulation_info.json", "w") as outfile:
            json.dump(folder_info, outfile, indent=2)

async def run(inputs: InputSchema, worker_nodes: List[str], orchestrator_node: str, flow_run: Any, cfg: Dict = None):
    logger.info(f"Running with inputs: {inputs}")
    logger.info(f"Worker nodes: {worker_nodes}")
    logger.info(f"Orchestrator node: {orchestrator_node}")

    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")

    sim_manager = SimulationManager(worker_nodes, orchestrator_node, flow_run)
    await sim_manager.init_simulation()
    await sim_manager.run_simulation(inputs.get('num_steps', NUM_STEPS))

    logger.info("Simulation completed successfully")