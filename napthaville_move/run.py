import os
import json
import math
import uuid
import shutil
import logging
from functools import partial
import ipfshttpclient
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from napthaville_move.schemas import InputSchema
from napthaville_move.maze import Maze
from naptha_sdk.task import Task as NapthaTask

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALL_PERSONAS = [
    "Isabella Rodriguez",
    "Maria Lopez",
    "Klaus Mueller"
]

BASE_SIMS_FOLDER = f"{os.getenv('BASE_OUTPUT_DIR')}/napthaville/step-3-3"
IPFS_GATEWAY_URL = os.getenv('IPFS_GATEWAY_URL')

class SimulationManager:
    def __init__(self, worker_nodes: List[str], orchestrator_node: str, flow_run: Any, num_steps: int, start_step: int):
        self.worker_nodes = worker_nodes
        self.orchestrator_node = orchestrator_node
        self.flow_run = flow_run
        self.persona_to_worker = self._assign_personas_to_workers()
        self.maze = Maze(maze_name="napthaville")
        self.sims_folders: Dict[str, str] = {}
        self.persona_tiles: Dict[str, tuple] = {}
        self.start_time: datetime
        self.curr_time: datetime
        self.sec_per_step: int
        self.num_steps: int = num_steps
        self.start_step: int = start_step
        self.maze_ipfs_hash: str = ""
        logger.info(f"Initializing simulation manager with {self.num_steps} steps. Start step: {self.start_step}")
        logger.info(f"Persona to worker: {self.persona_to_worker}")

    def _assign_personas_to_workers(self) -> Dict[str, str]:
        """Assign personas to workers in a round-robin fashion."""
        return {persona: self.worker_nodes[i % len(self.worker_nodes)] 
                for i, persona in enumerate(ALL_PERSONAS)}

    async def init_simulation(self):
        """Initialize the simulation environment."""
        self.orchestrator_sims_folder = self.fork_sims_folder()
        env, meta = self.load_initial_state()
        await self.init_workers(env)
        self.curr_time = datetime.strptime(meta['curr_time'], "%B %d, %Y, %H:%M:%S")
        self.sec_per_step = meta['sec_per_step']
        logger.info("Simulation initialization completed")

    def fork_sims_folder(self) -> str:
        """Create a new simulation folder."""
        new_sims_folder = Path(os.getenv('BASE_OUTPUT_DIR', '')) / str(uuid.uuid4())
        shutil.copytree(BASE_SIMS_FOLDER, new_sims_folder, dirs_exist_ok=True)
        return str(new_sims_folder)

    def load_initial_state(self) -> Tuple[Dict, Dict]:
        """Load initial environment and metadata."""
        with open(f"{self.orchestrator_sims_folder}/environment/{self.start_step}.json", 'r') as f:
            env = json.load(f)
        with open(f"{self.orchestrator_sims_folder}/reverie/meta.json", 'r') as f:
            meta = json.load(f)
        
        # Set the start_time and curr_time from the meta data
        self.start_time = datetime.strptime(f"{meta['start_date']}, 00:00:00",  "%B %d, %Y, %H:%M:%S")
        self.curr_time = datetime.strptime(meta['curr_time'], "%B %d, %Y, %H:%M:%S")
        self.sec_per_step = meta['sec_per_step']
        
        return env, meta

    async def init_workers(self, env: Dict):
        """Initialize the workers."""
        maze_json = self.maze.to_json()
        self.maze_ipfs_hash = self.upload_maze_json_to_ipfs(maze_json)
        
        for persona in ALL_PERSONAS:
            task = self.create_task("fork_persona", self.persona_to_worker[persona])
            response = await task(
                task = 'fork_persona',
                task_params = {
                    'persona_name': persona,
                    'maze_ipfs_hash': self.maze_ipfs_hash,
                    'curr_tile': (env[persona]['x'], env[persona]['y'])
                }
            )
            response_data = json.loads(response)
            self.sims_folders[persona] = response_data['sims_folder']
            self.persona_tiles[persona] = (env[persona]['x'], env[persona]['y'])
            self.maze_ipfs_hash = response_data['maze_ipfs_hash']

    def create_task(self, name: str, worker: str) -> NapthaTask:
        """Create a NapthaTask with common parameters."""
        return NapthaTask(
            name=name,
            fn='napthaville_module',
            worker_node=worker,
            orchestrator_node=self.orchestrator_node,
            flow_run=self.flow_run
        )

    async def run_simulation(self, num_steps: int):
        """Run the simulation for a specified number of steps."""
        self.num_steps = num_steps
        for step in range(self.num_steps):
            logger.info(f"Starting step {step + 1}")
            await self.process_step(step + self.start_step)
            self.curr_time += timedelta(seconds=self.sec_per_step)

        return self.save_final_state()

    async def process_step(self, step: int):
        """Process a single simulation step."""
        new_env = self.load_environment(step)
        personas_scratch = await self.get_all_persona_scratch()
        movements = await self.get_all_person_moves_v2(personas_scratch)
        personas_scratch = await self.get_all_persona_scratch()
        self.update_environment(new_env, personas_scratch)
        self.save_movements(step, movements)

    def load_environment(self, step: int) -> Dict:
        """Load the environment state for the current step."""
        with open(f"{self.orchestrator_sims_folder}/environment/{self.start_step + step}.json") as json_file:
            return json.load(json_file)

    async def get_all_persona_scratch(self) -> Dict[str, Dict]:
        """Get scratch data for all personas."""
        persona_scratch = {}
        for persona in ALL_PERSONAS:
            task = self.create_task("get_scratch", self.persona_to_worker[persona])
            response = await task(
                task='get_scratch',
                task_params={
                    'persona_name': persona,
                    'sims_folder': self.sims_folders[persona]
                }
            )
            persona_scratch[persona] = json.loads(response)
        return persona_scratch

    async def get_all_persona_moves(self, personas_scratch: Dict[str, Dict]) -> Dict[str, Dict]:
        moves = {}
        for persona in ALL_PERSONAS:
            task = self.create_task("get_move", self.persona_to_worker[persona])
            response = await task(
                task = 'get_move',
                task_params = {
                    'init_persona_name': persona,
                    'sims_folder': self.sims_folders[persona],
                    'personas': json.dumps(personas_scratch),
                    'curr_tile': self.persona_tiles[persona],
                    'curr_time': self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
                    'maze_ipfs_hash': self.maze_ipfs_hash
                }
            )
            moves[persona] = json.loads(response)
            
            # Update maze_ipfs_hash if the worker has updated it
            if 'maze_ipfs_hash' in moves[persona]:
                self.maze_ipfs_hash = moves[persona]['maze_ipfs_hash']
        
        return moves

    async def get_all_person_moves_v2(self, personas_scratch: Dict[str, Dict]) -> Dict[str, Dict]:
        """Get all persona moves."""
        moves = {}
        for persona in ALL_PERSONAS:
            # (1) get perceived and retrieved ; save the memory on persona  
            retrieved_task = self.create_task("get_retrieved", self.persona_to_worker[persona])
            retrieved_response = await retrieved_task(
                task = 'get_perceived_retrieved',
                task_params = {
                    'init_persona_name': persona,
                    'sims_folder': self.sims_folders[persona],
                    'curr_time': self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
                    'maze_ipfs_hash': self.maze_ipfs_hash,
                    'curr_tile': self.persona_tiles[persona]
                }
            )
            retrieved_data = json.loads(retrieved_response)
            logger.info(f"retrieved_data: {retrieved_data}")
            retrieved = retrieved_data['retrieved']
            new_day = retrieved_data['new_day']
            curr_time = retrieved_data['curr_time']
            
            # (2) get_reaction_mode 
            reaction_mode_task = self.create_task("get_reaction_mode", self.persona_to_worker[persona])
            reaction_mode_response = await reaction_mode_task(
                task = 'get_reaction_mode',
                task_params = {
                    'retrieved': json.dumps(retrieved),
                    'new_day': new_day,
                    'curr_time': curr_time,
                    'maze_ipfs_hash': self.maze_ipfs_hash,
                    'curr_tile': self.persona_tiles[persona],
                    'sims_folder': self.sims_folders[persona],
                    'personas': json.dumps(personas_scratch),
                    'init_persona_name': persona
                }
            )
            reaction_mode_data = json.loads(reaction_mode_response)
            logger.info(f"reaction_mode_data: {reaction_mode_data}")
            reaction_mode = reaction_mode_data['reaction_mode']
            focused_event = reaction_mode_data['focuesed_event']

            # (3) next step based on reaction_mode
            if reaction_mode:
                if reaction_mode[:9] == "chat with":
                    logger.info(f"chat with {reaction_mode[9:]}")   
                    persona_plan = await self.chat_react_plan(persona, reaction_mode)
                    logger.info(f"persona_plan: {persona_plan}")
                elif reaction_mode[:4] == "wait":
                    logger.info(f"wait {reaction_mode[5:]}")
                    persona_plan = await self.wait_react_plan(persona, reaction_mode)
                    logger.info(f"persona_plan: {persona_plan}")
            else:
                logger.info(f"no reaction")
                persona_plan = await self.no_reaction_plan(persona)
                logger.info(f"persona_plan: {persona_plan}")
    
            # (4) reflect and execute
            reflect_execute_task = self.create_task("reflect_execute", self.persona_to_worker[persona])
            reflect_execute_response = await reflect_execute_task(
                task = 'reflect_execute',
                task_params = {
                    'persona_name': persona,
                    'sims_folder': self.sims_folders[persona],
                    'persona_plan': persona_plan,
                    'personas_curr_tiles': self.persona_tiles,
                    'plan': persona_plan,
                    'maze_ipfs_hash': self.maze_ipfs_hash
                }
            )
            logger.info(f"reflect_execute_response: {reflect_execute_response}")
            reflect_execute_response = json.loads(reflect_execute_response)
            moves[persona] = reflect_execute_response['execution']
            self.maze_ipfs_hash = reflect_execute_response['maze_ipfs_hash']

        return moves

    async def no_reaction_plan(self, persona):
        complete_plan_task = NapthaTask(
            name = 'get_complete_plan',
            fn = 'napthaville_module',
            worker_node = self.persona_to_worker[persona],
            orchestrator_node = self.orchestrator_node,
            flow_run = self.flow_run,
        )
        complete_plan_response = await complete_plan_task(
            task="get_complete_plan_no_reaction",
            task_params={
                'persona_name': persona,
                'sims_folder': self.sims_folders[persona],
            }
        )
        return json.loads(complete_plan_response)
        

    async def wait_react_plan(self, persona, reaction_mode):
        wait_react_task = self.create_task("wait_react", self.persona_to_worker[persona])
        wait_react_response = await wait_react_task(
            task = 'get_complete_plan_wait',
            task_params = {
                'persona_name': persona,
                'sims_folder': self.sims_folders[persona],
                'reaction_mode': reaction_mode
            }
        )
        return json.loads(wait_react_response)

    async def chat_react_plan(self, init_persona_name, reaction_mode):
        target_persona_name = reaction_mode[9:].strip()        
        init_persona_node = self.persona_to_worker[init_persona_name]
        target_persona_node = self.persona_to_worker[target_persona_name]

        init_persona_info_task = NapthaTask(
        name = 'get_personal_info',
        fn = 'napthaville_module',
        worker_node = init_persona_node,
        orchestrator_node = self.orchestrator_node,
        flow_run = self.flow_run,
        )

        target_persona_info_task = NapthaTask(
            name = 'get_personal_info',
            fn = 'napthaville_module',
            worker_node = target_persona_node,
            orchestrator_node = self.orchestrator_node,
            flow_run = self.flow_run,
        )

        init_persona_info = await init_persona_info_task(
        task='get_personal_info', 
        task_params={
            'persona_name': init_persona_name,
            'sims_folder': self.sims_folders['init_persona_name']

        })
        init_persona_info = json.loads(init_persona_info)
        target_persona_info = await target_persona_info_task(
            task='get_personal_info', 
            task_params={
                'persona_name': target_persona_name,
            }
        )
        target_persona_info = json.loads(target_persona_info)
        init_persona_chat_params = {
            'init_persona_name': init_persona_name,
            'sims_folder': self.sims_folders['init_persona_name'],
            'target_persona_name': target_persona_info['name'],
            'target_persona_description': target_persona_info['act_description'],
        }

        target_persona_chat_params = {
            'init_persona_name': target_persona_name,
            'sims_folder': self.sims_folders['target_persona_name'],
            'target_persona_name': init_persona_info['name'],
            'target_persona_description': init_persona_info['act_description'],
        }

        curr_chat = []
        for i in range(8):
            init_persona_chat_params['curr_chat'] = json.dumps(curr_chat)
            init_utterance = await init_persona_info_task(
                task='get_utterence',
                task_params=init_persona_chat_params,
            )
            logger.info(f"init_utterance: {init_utterance}")

            target_persona_chat_params['curr_chat'] = json.dumps(json.loads(init_utterance)['curr_chat'])
            target_utterance = await target_persona_info_task(
                task='get_utterence',
                task_params=target_persona_chat_params,
            )
            logger.info(f"target_utterance: {target_utterance}")

            curr_chat = json.loads(target_utterance)['curr_chat']
            logger.info(f"curr_chat: {curr_chat}")

        all_utt = ""
        for row in curr_chat:
            speaker = row[0]
            utt = row[1]
            all_utt += f"{speaker}: {utt}\n"

        convo_length = math.ceil(int(len(all_utt) / 8) / 30)

        target_persona_scratch_task = NapthaTask(
            name = 'get_scratch',
            fn = 'napthaville_module',
            worker_node = target_persona_node,
            orchestrator_node = self.orchestrator_node,
            flow_run = self.flow_run,
        )

        target_persona_scratch = await target_persona_scratch_task(
            task='get_scratch',
            task_params={
                'persona_name': target_persona_name,
                'sims_folder': self.sims_folders[target_persona_name]
            }
        )

        complete_plan_task = NapthaTask(
            name = 'get_complete_plan',
            fn = 'napthaville_module',
            worker_node = init_persona_node,
            orchestrator_node = self.orchestrator_node,
            flow_run = self.flow_run,
        )

        complete_plan_response = await complete_plan_task(
            task="get_complete_plan",
            task_params={
                'all_utt': all_utt,
                'persona_name': init_persona_name,
                'sims_folder': self.sims_folders[init_persona_name],
                'target_persona_scratch': target_persona_scratch,
                'maze_ipfs_hash': self.maze_ipfs_hash,
                'convo_length': convo_length
            }
        )

        return json.loads(complete_plan_response)


    
    def update_environment(self, new_env: Dict, personas_scratch: Dict[str, Dict]):
        """Update the environment based on persona movements."""
        try:
            # Retrieve the current maze state from IPFS
            maze_json = self.retrieve_maze_json_from_ipfs(self.maze_ipfs_hash)
            maze = Maze.from_json(maze_json)

            for persona in ALL_PERSONAS:
                curr_tile = self.persona_tiles[persona]
                new_tile = (new_env[persona]['x'], new_env[persona]['y'])
                self.persona_tiles[persona] = new_tile

                # Remove the persona's events from the old tile
                maze.remove_subject_events_from_tile(persona, curr_tile)

                # Add the persona's new event to the new tile
                persona_event = self.get_persona_event(personas_scratch[persona])
                maze.add_event_from_tile(persona_event, new_tile)

            # Upload the updated maze back to IPFS
            updated_maze_json = maze.to_json()
            self.maze_ipfs_hash = self.upload_maze_json_to_ipfs(updated_maze_json)

            logger.info(f"Environment updated. New maze IPFS hash: {self.maze_ipfs_hash}")

        except Exception as e:
            logger.error(f"Error updating environment: {str(e)}")
            raise

        self.maze = maze

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

    def upload_maze_json_to_ipfs(self, maze_json: Dict[str, Any]) -> str:
        try:
            with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
                maze_json_str = json.dumps(maze_json)
                res = client.add_str(maze_json_str)
                return res
        except Exception as e:
            logger.error(f"Error uploading to IPFS: {str(e)}")
            raise

    def retrieve_maze_json_from_ipfs(self, ipfs_hash: str) -> Dict[str, Any]:
        try:
            with ipfshttpclient.connect(IPFS_GATEWAY_URL) as client:
                maze_json_str = client.cat(ipfs_hash)
                maze_json = json.loads(maze_json_str)
                return maze_json
        except Exception as e:
            logger.error(f"Error retrieving from IPFS: {str(e)}")
            raise

    def save_movements(self, step: int, movements: Dict[str, Dict]):
        """Save the movements for the current step."""
        movements["meta"] = {"curr_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S")}
        with open(f"{self.orchestrator_sims_folder}/movement/{step}.json", "w") as outfile:
            json.dump(movements, outfile, indent=2)

    def _serialize_node(self, node: Any) -> Dict[str, Any]:
        """Serialize a node object into a dictionary."""
        if hasattr(node, 'node_url'):
            return {'type': 'http', 'url': node.node_url}
        elif hasattr(node, 'indirect_node_id'):
            return {
                'type': 'ws',
                'indirect_node_id': node.indirect_node_id,
                'routing_url': node.routing_url
            }
        else:
            raise ValueError(f"Unknown node type: {node}")

    def save_final_state(self):
        """Save the final state of the simulation."""
        maze_json = self.retrieve_maze_json_from_ipfs(self.maze_ipfs_hash)
        with open(f"{self.orchestrator_sims_folder}/maze.json", "w") as outfile:
            json.dump(maze_json, outfile, indent=2)
        
        # Serialize worker nodes
        serialized_workers = {
            persona: self._serialize_node(node)
            for persona, node in self.persona_to_worker.items()
        }

        folder_info = {
            "orchestrator_sims_folder": Path(self.orchestrator_sims_folder).name,
            "personas": ALL_PERSONAS,
            "num_steps": self.num_steps,
            "start_time": self.start_time.strftime("%B %d, %Y, %H:%M:%S"),
            "end_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
            "sec_per_step": self.sec_per_step,
            "personas_to_worker": serialized_workers,
            "sims_folders": self.sims_folders,
            "final_maze_ipfs_hash": self.maze_ipfs_hash
        }
        with open(f"{self.orchestrator_sims_folder}/simulation_info.json", "w") as outfile:
            json.dump(folder_info, outfile, indent=2)

        logger.info(f"Final state saved to {self.orchestrator_sims_folder}")

        return folder_info

async def run(inputs: InputSchema, worker_nodes: List[str], orchestrator_node: str, flow_run: Any, cfg: Dict = None):
    logger.info(f"Running with inputs: {inputs}")
    logger.info(f"Worker nodes: {worker_nodes}")
    logger.info(f"Orchestrator node: {orchestrator_node}")

    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")
    
    start_step = inputs.start_step
    num_steps = inputs.num_steps

    sim_manager = SimulationManager(worker_nodes, orchestrator_node, flow_run, num_steps, start_step)
    await sim_manager.init_simulation()
    folder_info = await sim_manager.run_simulation(num_steps)

    logger.info("Simulation completed successfully")

    return folder_info