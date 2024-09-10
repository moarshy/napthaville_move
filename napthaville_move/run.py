import os
import json
import math
import uuid
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
from napthaville_move.schemas import InputSchema
from napthaville_move.maze import Maze
from napthaville_move.utils import (
    setup_logging, 
    BASE_MAZE_IPFS_HASH, 
    BASE_OUTPUT_DIR,
    get_folder_from_ipfs,
    retrieve_json_from_ipfs,
    upload_json_file_to_ipfs,
    upload_maze_json_to_ipfs
)
from naptha_sdk.task import Task as NapthaTask

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)

PERSONAS = {
    "Mark Schmidt": "QmXJBcUiMAg7CfNSaYyNH8Ap1hh15chVwPepGbiRckkSxe",
    "Mo Arshy": "QmXMTvYR8P9zNXtujWvcsggix3HGQxaM8KV2QKE3vD2nm2",
    "Richard Blythman": "QmeMDy7GhjVqQwKhUxcjPEqca8tZ5sfFQKBE473otakLgj",
}
BASE_SIMS_IPFS_HASH = "QmXhXrkrGMNukzFzRJMai2nKC2X181bFvZRDw27W2NyGH7"


class SimulationManager:
    def __init__(self, worker_nodes: List[str], orchestrator_node: str, flow_run: Any, num_steps: int, ipfs_hash: str = None):
        logger.info(f"Initializing SimulationManager with {num_steps} steps")
        self.num_steps = num_steps
        self.worker_nodes = worker_nodes
        self.orchestrator_node = orchestrator_node
        self.flow_run = flow_run
        self.game_obj_cleanup = {}

        if ipfs_hash:
            self.load_simulation_from_ipfs(ipfs_hash)
        else:
            self.maze_folder = f"{BASE_OUTPUT_DIR}/maze"
            if not os.path.exists(self.maze_folder):
                get_folder_from_ipfs(BASE_MAZE_IPFS_HASH, self.maze_folder)

            self.maze_folder = f"{self.maze_folder}/{BASE_MAZE_IPFS_HASH}/matrix"
            self.maze = Maze(maze_name="napthaville", maze_folder=self.maze_folder)
            self.sims_folders: Dict[str, str] = {}
            self.persona_tiles: Dict[str, tuple] = {}
            self.personas_to_workers: Dict[str, str] = self._assign_personas_to_workers()
            self.all_personas: List[str] = list(PERSONAS.keys())
            self.start_time: datetime
            self.curr_time: datetime
            self.sec_per_step: int
            self.step: Optional[int] = None

        self.chat_personas = {}
        self.chat = False

    def _assign_personas_to_workers(self) -> Dict[str, str]:
        """Assign personas to workers in a round-robin fashion."""
        return {persona: self.worker_nodes[i % len(self.worker_nodes)] 
                for i, persona in enumerate(list(PERSONAS.keys()))}
    
    def load_simulation_from_ipfs(self, ipfs_hash: str):
        """Load simulation state from IPFS"""
        logger.info(f"Loading simulation state from IPFS hash: {ipfs_hash}")
        simulation_info = retrieve_json_from_ipfs(ipfs_hash)
        self.orchestrator_sims_folder = simulation_info["orchestrator_sims_folder"]

        # If orchestrator_sims_folder is not set, raise an error
        if not self.orchestrator_sims_folder:
            raise ValueError("orchestrator_sims_folder is not set")
        
        self.all_personas = simulation_info["personas"]
        self.personas_to_workers = simulation_info["personas_to_workers"]
        self.sims_folders = simulation_info["sims_folders"]
        self.num_steps = simulation_info["num_steps"]
        self.start_time = datetime.strptime(
            simulation_info["start_time"], "%B %d, %Y, %H:%M:%S"
        )
        self.curr_time = datetime.strptime(
            simulation_info["end_time"], "%B %d, %Y, %H:%M:%S"
        )
        self.sec_per_step = simulation_info["sec_per_step"]
        self.sims_folders = simulation_info["sims_folders"]
        self.maze_ipfs_hash = simulation_info["maze_ipfs_hash"]
        self.step = simulation_info["current_step"]

        # Load the last environment state
        last_env_file = f"{self.orchestrator_sims_folder}/environment/{self.step}.json"
        with open(last_env_file, "r") as f:
            last_env = json.load(f)

        # Set persona tiles based on the last environment state
        self.persona_tiles = {
            persona: (data["x"], data["y"]) for persona, data in last_env.items()
        }

        # Load the maze state using the IPFS hash
        maze_json = retrieve_json_from_ipfs(self.maze_ipfs_hash)
        self.maze = Maze.from_json(maze_json)
        logger.info("Simulation state loaded successfully from IPFS")

    async def init_simulation(self):
        """Initialize the simulation environment."""
        self.orchestrator_sims_folder = self.fork_sims_folder()
        env = self.load_initial_state()
        await self.init_workers(env)
        logger.info("Simulation initialization completed")

    def fork_sims_folder(self) -> str:
        """Create a new simulation folder."""
        logger.info("Creating new simulation folder")
        folder_id = str(uuid.uuid4())
        new_sims_folder = f"{BASE_OUTPUT_DIR}/{folder_id}"
        get_folder_from_ipfs(BASE_SIMS_IPFS_HASH, new_sims_folder)
        new_sims_folder = f"{new_sims_folder}/{BASE_SIMS_IPFS_HASH}"
        logger.info(f"New simulation folder created at: {new_sims_folder}")
        return new_sims_folder

    def load_initial_state(self) -> Tuple[Dict, Dict]:
        """Load initial environment and metadata."""
        logger.info("Loading initial state")
        with open(f"{self.orchestrator_sims_folder}/reverie/meta.json", "r") as f:
            meta = json.load(f)

        self.start_time = datetime.strptime(
            f"{meta['start_date']}, 00:00:00", "%B %d, %Y, %H:%M:%S"
        )
        # self.curr_time = datetime.strptime(meta["curr_time"], "%B %d, %Y, %H:%M:%S")
        # for testing can we set time to 7am
        self.curr_time = datetime.strptime(
            f"{meta['start_date']}, 09:00:00", "%B %d, %Y, %H:%M:%S"
        )
        # self.sec_per_step = meta["sec_per_step"]
        self.sec_per_step = 10 * 60
        self.step = meta["step"]
        self.all_personas = meta["persona_names"]
        logger.info(
            f"Initial state loaded. Start time: {self.start_time}, Current time: {self.curr_time}"
        )

        with open(
            f"{self.orchestrator_sims_folder}/environment/{self.step}.json", "r"
        ) as f:
            env = json.load(f)
        logger.info(f"Environment loaded for step {self.step}")
        return env

    async def init_workers(self, env: Dict):
        """Initialize the workers."""
        logger.info("Initializing workers")
        maze_json = self.maze.to_json()
        self.maze_ipfs_hash = upload_json_file_to_ipfs(maze_json)
        
        for persona in self.all_personas:
            task = self.create_task("fork_persona", self.personas_to_workers[persona])
            response = await task(
                task="prepare_persona",
                task_params={
                    "persona_name": persona,
                    "persona_ipfs_hash": PERSONAS[persona],
                    "curr_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
                    "maze_ipfs_hash": self.maze_ipfs_hash,
                    "curr_tile": (env[persona]["x"], env[persona]["y"]),
                },
            )
            response_data = json.loads(response)
            self.sims_folders[persona] = response_data['sims_folder']
            self.persona_tiles[persona] = (env[persona]['x'], env[persona]['y'])
            self.maze_ipfs_hash = response_data['maze_ipfs_hash']
            logger.info(f"Persona {persona} initialized at {self.sims_folders[persona]}. Tile: {self.persona_tiles[persona]}")

    def create_task(self, name: str, worker: str) -> NapthaTask:
        """Create a NapthaTask with common parameters."""
        return NapthaTask(
            name=name,
            fn='napthaville_module',
            worker_node=worker,
            orchestrator_node=self.orchestrator_node,
            flow_run=self.flow_run
        )

    async def run_simulation(self):
        """Run the simulation for a specified number of steps."""
        logger.info(
            f"Starting simulation from step {self.step} for {self.num_steps} steps"
        )
        for _ in range(self.num_steps):
            logger.info(f"Processing step {self.step} of {self.num_steps}")
            await self.process_step()
            logger.info(f"Step {self.step} completed. Current time: {self.curr_time}")

        return self.save_state(final_step=True)

    def save_state(self, final_step=None):
        """Save the state of the simulation."""
        logger.info("Saving simulation state")

        folder_info = {
            "orchestrator_sims_folder": self.orchestrator_sims_folder,
            "personas": self.all_personas,
            "num_steps": self.num_steps,
            "start_time": self.start_time.strftime("%B %d, %Y, %H:%M:%S"),
            "end_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S"),
            "sec_per_step": self.sec_per_step,
            "sims_folders": self.sims_folders,
            "maze_ipfs_hash": self.maze_ipfs_hash,
            "current_step": self.step,
            "personas_to_workers": self.personas_to_workers,
            "persona_tiles": self.persona_tiles,
        }
        with open(
            f"{self.orchestrator_sims_folder}/simulation_info.json", "w"
        ) as outfile:
            json.dump(folder_info, outfile, indent=2)

        logger.info(f"State saved to {self.orchestrator_sims_folder}")

        if final_step:
            final_json_ipfs_hash = upload_json_file_to_ipfs(
                f"{self.orchestrator_sims_folder}/simulation_info.json"
            )
            return folder_info, final_json_ipfs_hash
        return folder_info, None
    
    def save_environment(self, step: int, movements: Dict[str, List]):
        """Save the environment state for the current step."""
        logger.info(f"Saving environment for step {step}")
        new_env = {}
        for persona, data in movements.items():
            new_env[persona] = {
                'maze': 'the_ville',
                'x': data[0][0],
                'y': data[0][1]
            }

        with open(f"{self.orchestrator_sims_folder}/environment/{step}.json", "w") as outfile:
            json.dump(new_env, outfile, indent=2)
        logger.info(f"Environment saved for step {step}")

    async def process_step(self):
        """Process a single simulation step."""
        logger.info(f"Processing step {self.step}")
        # Load environment
        new_env = self.load_environment()

        # <game_obj_cleanup> cleanup
        for key, value in self.game_obj_cleanup.items():
            self.maze.turn_event_from_tile_idle(key, value)

        self.game_obj_cleanup = {}

        # Get all persona scratch
        self.personas_scratch = await self.get_all_persona_scratch()

        # Get curr tiles
        for persona in self.all_personas:
            curr_tile = self.persona_tiles[persona]
            new_tile = (new_env[persona]["x"], new_env[persona]["y"])
            self.persona_tiles[persona] = new_tile
            self.maze.remove_subject_events_from_tile(persona, curr_tile)
            self.maze.add_event_from_tile(
                self.get_curr_event_and_desc(self.personas_scratch[persona]), new_tile
            )

            if not self.personas_scratch[persona]["planned_path"]:
                self.game_obj_cleanup[
                    self.get_curr_obj_event_and_desc(self.personas_scratch[persona])
                ] = new_tile
                self.maze.add_event_from_tile(
                    self.get_curr_obj_event_and_desc(self.personas_scratch[persona]),
                    new_tile,
                )
                blank = (
                    self.get_curr_obj_event_and_desc(self.personas_scratch[persona])[0],
                    None,
                    None,
                    None,
                )
                self.maze.remove_event_from_tile(blank, new_tile)

        # Upload maze to IPFS
        self.maze_ipfs_hash = upload_maze_json_to_ipfs(self.maze.to_json())

        # Get all movements
        movements = await self.get_all_person_moves_v2(self.personas_scratch)
        logger.info(f"Movements: {movements}")

        self.personas_scratch = await self.get_all_persona_scratch()
        self.update_environment(new_env, self.personas_scratch)
        self.save_movements(self.step, movements)

        self.step += 1
        self.curr_time += timedelta(seconds=self.sec_per_step)
        self.save_environment(self.step, movements)
        self.save_state()
        logger.info(f"Step {self.step} processing completed")

    def load_environment(self) -> Dict:
        """Load the environment state for the current step."""
        logger.info(f"Loading environment for step {self.step}")
        with open(
            f"{self.orchestrator_sims_folder}/environment/{self.step}.json"
        ) as json_file:
            env = json.load(json_file)
        logger.info(f"Environment loaded for step {self.step}")
        return env

    async def get_all_persona_scratch(self) -> Dict[str, Dict]:
        """Get scratch data for all personas."""
        logger.info("Fetching scratch data for all personas")
        persona_scratch = {}
        for persona in self.all_personas:
            task = self.create_task("get_scratch", self.personas_to_workers[persona])
            response = await task(
                task='get_scratch',
                task_params={
                    'persona_name': persona,
                    'sims_folder': self.sims_folders[persona]
                }
            )
            persona_scratch[persona] = json.loads(response)
            logger.info(f"Persona {persona} scratch data loaded")
        return persona_scratch

    async def get_all_person_moves_v2(self, personas_scratch: Dict[str, Dict]) -> Dict[str, Dict]:
        """Get all persona moves."""
        logger.info("Starting get_all_person_moves_v2")
        moves = {}
        for persona in  self.all_personas:
            # (1) get perceived and retrieved ; save the memory on persona  
            retrieved_task = self.create_task("get_perceived_retrieved", self.personas_to_workers[persona])
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
            reaction_mode_task = self.create_task("get_reaction_mode", self.personas_to_workers[persona])
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
            focused_event = reaction_mode_data['focused_event']

            # (3) next step based on reaction_mode
            if reaction_mode:
                if reaction_mode[:9] == "chat with":
                    logger.info(f"{persona} will chat with {reaction_mode[9:]}")   
                    self.chat = True
                    self.chat_personas = [persona, reaction_mode[9:].strip()]
                    persona_plan = await self.chat_react_plan(persona, reaction_mode)
                    logger.info(f"persona_plan: {persona_plan}")
                elif reaction_mode[:4] == "wait":
                    logger.info(f"{persona} will wait {reaction_mode[5:]}")
                    persona_plan = await self.wait_react_plan(persona, reaction_mode)
                    logger.info(f"persona_plan: {persona_plan}")
            else:
                logger.info(f"No reaction for {persona}")
                persona_plan = await self.no_reaction_plan(persona)
                logger.info(f"persona_plan: {persona_plan}")
    
            # (4) reflect and execute
            reflect_execute_task = self.create_task("reflect_execute", self.personas_to_workers[persona])
            reflect_execute_response = await reflect_execute_task(
                task = 'get_reflect_execute',
                task_params = {
                    'init_persona_name': persona,
                    'sims_folder': self.sims_folders[persona],
                    'personas_curr_tiles': self.persona_tiles,
                    'plan': persona_plan,
                    'maze_ipfs_hash': self.maze_ipfs_hash
                }
            )
            logger.info(f"reflect_execute_response: {reflect_execute_response}")
            reflect_execute_response = json.loads(reflect_execute_response)
            moves[persona] = reflect_execute_response['execution']
            self.maze_ipfs_hash = reflect_execute_response['maze_ipfs_hash']

        logger.info("Completed get_all_person_moves_v2")
        return moves

    async def no_reaction_plan(self, persona):
        complete_plan_task = self.create_task("get_complete_plan_no_reaction", self.personas_to_workers[persona])
        complete_plan_response = await complete_plan_task(
            task="get_complete_plan_no_reaction",
            task_params={
                'init_persona_name': persona,
                'sims_folder': self.sims_folders[persona],
            }
        )
        return json.loads(complete_plan_response)
        
    async def wait_react_plan(self, persona, reaction_mode):
        wait_react_task = self.create_task("get_complete_plan_wait", self.personas_to_workers[persona])
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
        init_persona_node = self.personas_to_workers[init_persona_name]
        target_persona_node = self.personas_to_workers[target_persona_name]

        init_persona_info_task = self.create_task("get_personal_info", init_persona_node)
        target_persona_info_task = self.create_task("get_personal_info", target_persona_node)
        
        init_persona_info = await init_persona_info_task(   
            task='get_personal_info', 
            task_params={
                'init_persona_name': init_persona_name,
                'sims_folder': self.sims_folders[init_persona_name]
            }
        )
        init_persona_info = json.loads(init_persona_info)
        target_persona_info = await target_persona_info_task(
            task='get_personal_info', 
            task_params={
                'init_persona_name': target_persona_name,
                'sims_folder': self.sims_folders[target_persona_name]
            }
        )
        target_persona_info = json.loads(target_persona_info)
        init_persona_chat_params = {
            'init_persona_name': init_persona_name,
            'sims_folder': self.sims_folders[init_persona_name],
            'target_persona_name': target_persona_info['name'],
            'target_persona_description': target_persona_info['act_description'],
        }

        target_persona_chat_params = {
            'init_persona_name': target_persona_name,
            'sims_folder': self.sims_folders[target_persona_name],
            'target_persona_name': init_persona_info['name'],
            'target_persona_description': init_persona_info['act_description'],
        }

        init_utt_task = self.create_task("get_utterence", init_persona_node)
        target_utt_task = self.create_task("get_utterence", target_persona_node)

        curr_chat = []
        for i in range(8):
            init_persona_chat_params['curr_chat'] = json.dumps(curr_chat)
            init_utterance = await init_utt_task(
                task='get_utterence',
                task_params=init_persona_chat_params,
            )
            logger.info(f"init_utterance: {init_utterance}")

            target_persona_chat_params['curr_chat'] = json.dumps(json.loads(init_utterance)['curr_chat'])
            target_utterance = await target_utt_task(
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

        target_persona_scratch_task = self.create_task("get_scratch", target_persona_node)
        target_persona_scratch = await target_persona_scratch_task(
            task = 'get_scratch',
            task_params = {
                'persona_name': target_persona_name,
                'sims_folder': self.sims_folders[target_persona_name]
            }
        )

        complete_plan_task = self.create_task("get_complete_plan", init_persona_node)

        complete_plan_response = await complete_plan_task(
            task="get_complete_plan",
            task_params={
                'all_utt': all_utt,
                'init_persona_name': init_persona_name,
                'sims_folder': self.sims_folders[init_persona_name],
                'target_persona_scratch': target_persona_scratch,
                'maze_ipfs_hash': self.maze_ipfs_hash,
                'convo_length': convo_length
            }
        )
        complete_plan_response = json.loads(complete_plan_response)
        init_persona_act_address = complete_plan_response['init_persona_act_address']
        target_persona_return = complete_plan_response["target_persona_return"]

        finalise_target_persona_task = self.create_task("finalise_target_persona", target_persona_node)
        finalise_target_persona_response = await finalise_target_persona_task(
            task = 'finalise_target_persona_chat',
            task_params = {
                'target_persona_name': target_persona_name,
                'sims_folder': self.sims_folders[target_persona_name],
                'target_persona_return': target_persona_return
            }
        )
        finalise_target_persona_response = json.loads(finalise_target_persona_response)
        logger.info(f"finalise_target_persona_response: {finalise_target_persona_response}")

        return init_persona_act_address

    def update_environment(self, new_env: Dict, personas_scratch: Dict[str, Dict]):
        """Update the environment based on persona movements."""
        try:
            # Retrieve the current maze state from IPFS
            maze_json = retrieve_json_from_ipfs(self.maze_ipfs_hash)
            maze = Maze.from_json(maze_json)

            for persona in self.all_personas:
                curr_tile = self.persona_tiles[persona]
                new_tile = (new_env[persona]['x'], new_env[persona]['y'])
                self.persona_tiles[persona] = new_tile

                # Remove the persona's events from the old tile
                maze.remove_subject_events_from_tile(persona, curr_tile)

                # Add the persona's new event to the new tile
                persona_event = self.get_curr_event_and_desc(personas_scratch[persona])
                maze.add_event_from_tile(persona_event, new_tile)

            # Upload the updated maze back to IPFS
            updated_maze_json = maze.to_json()
            self.maze_ipfs_hash = upload_maze_json_to_ipfs(updated_maze_json)

            logger.info(f"Environment updated. New maze IPFS hash: {self.maze_ipfs_hash}")

        except Exception as e:
            logger.error(f"Error updating environment: {str(e)}")
            raise

        self.maze = maze

    def get_curr_event_and_desc(
        self, persona_scratch: Dict
    ) -> Tuple[str, Any, Any, Any]:
        """Get the current event and description for a persona."""
        act_address = persona_scratch.get("act_address")
        if not act_address:
            logger.info("No act_address found for persona. Returning empty event.")
            return (persona_scratch["name"], None, None, None)
        else:
            return (
                persona_scratch["act_event"][0],
                persona_scratch["act_event"][1],
                persona_scratch["act_event"][2],
                persona_scratch["act_description"],
            )

    def get_curr_obj_event_and_desc(
        self, persona_scratch: Dict
    ) -> Tuple[str, Any, Any, Any]:
        """Get the current object event and description for a persona."""
        act_address = persona_scratch.get("act_address")
        if not act_address:
            logger.info("No act_address found for persona. Returning empty event.")
            return ("", None, None, None)
        return (
            act_address,
            persona_scratch["act_obj_event"][1],
            persona_scratch["act_obj_event"][2],
            persona_scratch["act_obj_description"],
        )

    def save_movements(self, step: int, movements: Dict[str, Dict]):
        """Save the movements for the current step."""
        logger.info(f"Saving movements for step {step}")

        formatted_movements = {
            "persona": {},
            "meta": {"curr_time": self.curr_time.strftime("%B %d, %Y, %H:%M:%S")},
        }

        for persona, data in movements.items():
            formatted_movements["persona"][persona] = {
                "movement": data[0],
                "pronunciation": data[1],
                "description": data[2],
                "chat": self.personas_scratch[persona]["chat"],
            }

        if self.chat:
            formatted_movements["persona"][self.chat_personas[1].strip()]["chat"] = (
                self.personas_scratch[self.chat_personas[0].strip()]["chat"]
            )

        self.chat = False
        self.chat_personas = []

        if not os.path.exists(f"{self.orchestrator_sims_folder}/movement"):
            os.makedirs(f"{self.orchestrator_sims_folder}/movement", exist_ok=True)

        with open(
            f"{self.orchestrator_sims_folder}/movement/{step}.json", "w"
        ) as outfile:
            json.dump(formatted_movements, outfile, indent=2)

        logger.info(f"Movements saved for step {step}")


async def run(inputs: InputSchema, worker_nodes: List[str], orchestrator_node: str, flow_run: Any, cfg: Dict = None):
    logger.info(f"Running with inputs: {inputs}")
    logger.info(f"Worker nodes: {worker_nodes}")
    logger.info(f"Orchestrator node: {orchestrator_node}")

    if len(worker_nodes) < 1:
        raise ValueError("There should be at least 1 worker node available")
    
    num_steps = inputs.num_steps
    sims_ipfs_hash = inputs.sims_ipfs_hash

    sim_manager = SimulationManager(
        worker_nodes,
        orchestrator_node,
        flow_run,
        num_steps,
        sims_ipfs_hash,
    )
    if not sims_ipfs_hash:
        await sim_manager.init_simulation()
    folder_info, sims_ipfs_hash = await sim_manager.run_simulation()

    logger.info("Simulation completed successfully")

    return json.dumps({"folder_info": folder_info, "sims_ipfs_hash": sims_ipfs_hash})