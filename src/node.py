from concurrent.futures import ThreadPoolExecutor
import argparse, grpc, logging, os, sys, threading, time, random, math, psutil
from collections import deque, defaultdict

from . import dwse_pb2 as pb
from . import dwse_pb2_grpc as pb_grpc


def setup_logging(level_name="INFO"):
    log_level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=log_level,
                        format="%(asctime)s  [%(levelname)s] [Node %(node)s] %(message)s",
                        datefmt="%H:%M:%S")

def log(msg, lvl=logging.INFO, node=-1):
    logging.log(lvl, msg, extra={'node': node})

def parse_peers(arg):
    peers={}
    if arg:
        for entry in arg.split(','):
            nid, host, port = entry.split(':')
            peers[int(nid)] = f"{host}:{port}"
    return peers

class PeerStats:
    def __init__(self, weight=0, queue_length=0, specialization=None):
        self.weight = weight
        self.queue_length = queue_length
        self.specialization = specialization or {}

#  gRPC Servicer 
class Servicer(pb_grpc.DwseNodeServicer):
    def __init__(self, node):
        self.node = node
        self.task_executor = ThreadPoolExecutor(max_workers=256)
        
    def Heartbeat(self, req, _): 
        self.node.on_heartbeat(req)
        return pb.HeartbeatResponse(
            follower_id=self.node.id, 
            follower_weight=self.node.weight, 
            ok=True,
            stats=pb.NodeStats(
                node_id=self.node.id,
                queue_length=len(self.node.task_queue),
                cpu_usage=self.node.get_cpu_usage(),
                memory_available=self.node.get_available_memory(),
                avg_response_time=self.node.avg_response_time,
                tasks_completed=self.node.tasks_completed,
                tasks_failed=self.node.tasks_failed,
                specialization={t: float(s/self.node.task_type_attempts[t]) 
                               for t, s in self.node.task_type_success.items() 
                               if self.node.task_type_attempts[t] > 0}
            )
        )
        
    def Election(self, req, _): 
        return pb.ElectionResponse(ok=self.node.on_election(req), responder_id=self.node.id, responder_weight=self.node.weight)
        
    def SubmitTask(self, req, ctx): 
        if not self.node.is_leader:
            ctx.set_details(f"NotLeader; leader_id={self.node.leader_id or -1}")
            ctx.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return pb.TaskResult(task_id=req.task_id, result="", node_id=self.node.id)
            
        future = self.task_executor.submit(self.node.assign_task, req)
        return future.result()
        
    def ExecuteTask(self, req, _): 
        return self.node.exec_task(req)
        
    def StealTasks(self, req, _): 
        return self.node.handle_steal_request(req)

class Node:
    def __init__(self, nid, addr, peers, mode, advanced_weight=False, enable_stealing=False):
        self.id=nid; self.addr=addr; self.peers=peers; self.mode=mode
        self.advanced_weight = advanced_weight
        self.enable_stealing = enable_stealing
        
        self.weight=int(max(0, 10-nid))       
        self.term=0; self.is_leader=False; self.leader_id=None
        self.last_hb=time.time()
        
       
        
        # Task queue and performance metrics
        self.task_queue = deque()
        self.tasks_completed = 0
        self.tasks_attempted = 0
        self.tasks_failed = 0
        self.response_times = deque(maxlen=100) 
        self.avg_response_time = 0
        self.last_activity_time = time.time()
        
        
        self.task_type_attempts = defaultdict(int)
        self.task_type_success = defaultdict(int)
        self.current_task_type = "default"
        
        
        self.min_queue_threshold = 5  
        self.min_steal_threshold = 1  
        self.max_steal_count = 10    
        self.steal_cooldown = 0.5     
        self.last_steal_attempt = 0
        self.successful_steals = 0
        self.times_stolen_from = 0
        
     
        self.peer_stats = {}
        self.network_distances = {}
        
        
        self.server=grpc.server(ThreadPoolExecutor(max_workers=256))
        pb_grpc.add_DwseNodeServicer_to_server(Servicer(self), self.server)
        self.server.add_insecure_port(addr)
        
      
        self.stubs={pid:pb_grpc.DwseNodeStub(grpc.insecure_channel(a))
                    for pid,a in peers.items() if pid!=nid}

    # Advanced Weight Calculation
    def get_cpu_usage(self):
      
        try:
            return psutil.cpu_percent(interval=0.1)
        except:
            return random.uniform(20, 80)
    
    def get_available_memory(self):
        try:
            return psutil.virtual_memory().available
        except:
            return random.uniform(1024*1024*100, 1024*1024*1000)  # 100MB to 1GB
    

    def get_network_bandwidth(self):
        try:
            net_io = psutil.net_io_counters()
            bandwidth = (net_io.bytes_sent + net_io.bytes_recv) / 1024 / 1024  # Convert to MB
            return min(100.0, bandwidth)
        except:
            cpu_usage = self.get_cpu_usage()
            return max(10.0, 100.0 - (cpu_usage / 2))
    
    def measure_average_latency(self):
        if hasattr(self, 'heartbeat_response_times') and self.heartbeat_response_times:
            return sum(self.heartbeat_response_times) / len(self.heartbeat_response_times)
        elif self.peer_stats:
            avg_queue_length = sum(stats.queue_length for stats in self.peer_stats.values()) / max(1, len(self.peer_stats))
            return 0.01 + (avg_queue_length * 0.001) 
        else:
            cpu_usage = self.get_cpu_usage()
            return 0.01 + (cpu_usage / 1000) 
    
    def calculate_capacity_factor(self):
        cpu_usage = self.get_cpu_usage()  
        memory_available = self.get_available_memory() 
        network_bandwidth = self.get_network_bandwidth() 
        
       
        cpu_factor = 1 - min(cpu_usage / 100.0, 1.0)
        memory_factor = min(memory_available / (1024*1024*1000), 1.0) 
        network_factor = min(network_bandwidth / 100.0, 1.0) 
        
       
        return 0.4 * cpu_factor + 0.3 * memory_factor + 0.3 * network_factor
    
    def calculate_history_factor(self):
        return 2.0 / (1.0 + math.exp(-0.01 * self.tasks_completed)) - 1.0
    
    def calculate_queue_factor(self):
        queue_length = len(self.task_queue)
        return 1.0 / (1.0 + 0.1 * queue_length)
    
    def calculate_response_factor(self):
        if not self.avg_response_time:
            return 1.0
        return math.exp(-0.1 * self.avg_response_time)
    
    def calculate_reliability_factor(self):
        if self.tasks_attempted == 0:
            return 1.0
        failure_rate = self.tasks_failed / self.tasks_attempted
        return 1.0 - failure_rate
    
    def calculate_time_decay(self):
        current_time = time.time()
        time_since_last_activity = current_time - self.last_activity_time
        return math.exp(-0.01 * time_since_last_activity)
    
    def calculate_distance_factor(self):
        avg_latency = self.measure_average_latency()
        return 1.0 / (1.0 + 0.1 * avg_latency)
    
    def calculate_specialization_factor(self, task_type):
        if task_type not in self.task_type_success or self.task_type_attempts[task_type] == 0:
            return 0.5 
        success_rate = self.task_type_success[task_type] / self.task_type_attempts[task_type]
        return success_rate
    
    def calculate_advanced_weight(self):
        capacity = self.calculate_capacity_factor()
        history = self.calculate_history_factor()
        queue = self.calculate_queue_factor()
        response = self.calculate_response_factor()
        reliability = self.calculate_reliability_factor()
        time_decay = self.calculate_time_decay()
        distance = self.calculate_distance_factor()
        specialization = self.calculate_specialization_factor(self.current_task_type)
        
        weight = (
            0.20 * capacity +
            0.15 * history +
            0.10 * queue +
            0.15 * response +
            0.20 * reliability +
            0.05 * time_decay +
            0.10 * distance +
            0.05 * specialization
        )
        
        return int(weight * 100)
    
    def update_weight(self):
        """Update node weight based on configuration."""
        if self.advanced_weight:
            self.weight = self.calculate_advanced_weight()
        self.weight = int(self.weight)
    
    def attempt_steal_work(self):
        self.weight = int(self.weight)
        
        if len(self.task_queue) >= self.min_queue_threshold:
            return False
            
        current_time = time.time()
        if current_time - self.last_steal_attempt < self.steal_cooldown:
            return False
        self.last_steal_attempt = current_time
            
        potential_victims = []
        for pid in self.peers.keys():
            if pid == self.id:
                continue
                
            if pid in self.peer_stats:
                queue_length = self.peer_stats[pid].queue_length
                potential_victims.append((pid, queue_length))
            else:
                potential_victims.append((pid, self.min_steal_threshold + 1))
                
        potential_victims.sort(key=lambda x: -x[1])
        
        for victim_id, queue_length in potential_victims:
            if queue_length <= self.min_steal_threshold:
                continue  # Don't steal from nodes with few tasks
                
            if victim_id not in self.stubs:
                log(f"Cannot steal from Node {victim_id}: no stub available", node=self.id, lvl=logging.WARNING)
                continue
                
            try:
                stolen_tasks = self.stubs[victim_id].StealTasks(
                    pb.StealRequest(
                        thief_id=self.id,
                        max_tasks=self.max_steal_count
                    ),
                    timeout=0.5
                )
                
                if stolen_tasks and len(stolen_tasks.tasks) > 0:
                    for task in stolen_tasks.tasks:
                        self.task_queue.append(task)
                    self.update_weight_after_stealing(len(stolen_tasks.tasks))
                    log(f"Stole {len(stolen_tasks.tasks)} tasks from Node {victim_id}", node=self.id)
                    return True
            except grpc.RpcError as e:
                log(f"Error stealing from Node {victim_id}: {e}", node=self.id, lvl=logging.DEBUG)
                continue
                
        return False
    
    def handle_steal_request(self, req):
        self.weight = int(self.weight)
        
        thief_id = req.thief_id
        max_tasks = req.max_tasks
        
        if len(self.task_queue) <= self.min_steal_threshold:
            return pb.StolenTasks(tasks=[])
            
        available = len(self.task_queue) - self.min_steal_threshold
        steal_count = min(available, max_tasks)
        
        stolen = []
        for _ in range(steal_count):
            if self.task_queue:
                task_req = self.task_queue.popleft()
                task_assignment = pb.TaskAssignment(
                    task_id=task_req.task_id,
                    payload=task_req.payload,
                    task_type=getattr(task_req, 'task_type', 'default')
                )
                stolen.append(task_assignment)
                
        if stolen:
            self.update_weight_after_being_stolen_from(len(stolen))
            log(f"Node {thief_id} stole {len(stolen)} tasks from us", node=self.id)
            
        return pb.StolenTasks(tasks=stolen)
    
    def update_weight_after_stealing(self, stolen_count):
        """Update weight after successfully stealing tasks."""
        if stolen_count > 0:
            self.weight = int(self.weight + 0.2 * stolen_count)
            self.successful_steals += stolen_count
            
    def update_weight_after_being_stolen_from(self, stolen_count):
        """Update weight after having tasks stolen."""
        self.weight = int(max(1, self.weight - 0.1 * stolen_count))  # Ensure weight is at least 1
        self.times_stolen_from += 1
    
    def on_heartbeat(self, hb):
        self.weight = int(self.weight)
        if self.leader_id != hb.leader_id:
            log(f"New leader announced: {hb.leader_id}",
                node=self.id, lvl=logging.INFO)
        self.leader_id = hb.leader_id
        self.term      = hb.term
        self.last_hb   = time.time()
        self.is_leader = (hb.leader_id == self.id)
        
        if hb.leader_id != self.id:  
            # Check if the heartbeat has stats
            try:
                if hasattr(hb, 'stats') and hb.stats:
                    stats = hb.stats
                    
                    self.peer_stats[hb.leader_id] = PeerStats(
                        weight=hb.leader_weight,
                        queue_length=stats.queue_length,
                        specialization={k: v for k, v in stats.specialization.items()}
                    )
                    
                    log(f"Updated stats for node {hb.leader_id}: weight={hb.leader_weight}, queue_length={stats.queue_length}", 
                        node=self.id, lvl=logging.DEBUG)
                else:
                    self.peer_stats[hb.leader_id] = PeerStats(
                        weight=hb.leader_weight,
                        queue_length=0
                    )
                    
                    log(f"Updated basic stats for node {hb.leader_id}: weight={hb.leader_weight}", 
                        node=self.id, lvl=logging.DEBUG)
            except Exception as e:
                log(f"Error updating peer stats: {e}", node=self.id, lvl=logging.ERROR)
                self.peer_stats[hb.leader_id] = PeerStats(
                    weight=hb.leader_weight,
                    queue_length=0
                )

    def on_election(self, req):
        self.weight = int(self.weight)
        log(f"Received election request from node {req.candidate_id} with weight {req.candidate_weight}", node=self.id, lvl=logging.INFO)
        higher=self.weight>req.candidate_weight or (self.weight==req.candidate_weight and self.id<req.candidate_id)
        log(f"My weight: {self.weight}, candidate weight: {req.candidate_weight}, I have higher priority: {higher}", node=self.id, lvl=logging.INFO)
        
        if higher and not self.is_leader:
            log(f"I have higher priority, starting my own election", node=self.id, lvl=logging.INFO)
            threading.Thread(target=self.start_election, daemon=True).start()
        
        return higher

    def handle_submit(self, req, ctx):
        self.weight = int(self.weight)
        
        if not self.is_leader:
            ctx.set_details(f"NotLeader; leader_id={self.leader_id or -1}")
            ctx.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return pb.TaskResult(task_id=req.task_id,result="",node_id=self.id)
        return self.assign_task(req)

    def exec_task(self, req):
        self.weight = int(self.weight)
        
        start_time = time.time()
        self.tasks_attempted += 1
        self.last_activity_time = start_time
        
        task_type = getattr(req, 'task_type', 'default')
        self.current_task_type = task_type
        self.task_type_attempts[task_type] += 1
        
        try:
            result = self.perform(req.payload)
            
            self.tasks_completed += 1
            self.task_type_success[task_type] += 1
            
            end_time = time.time()
            response_time = end_time - start_time
            self.response_times.append(response_time)
            self.avg_response_time = sum(self.response_times) / len(self.response_times)
            
            if self.advanced_weight:
                self.update_weight()
            else:
                self.weight = int(self.weight + 1)
                
            return pb.TaskResult(task_id=req.task_id, result=result, node_id=self.id)
        except Exception as e:
            self.tasks_failed += 1
            log(f"Task execution failed: {e}", node=self.id, lvl=logging.ERROR)
            return pb.TaskResult(task_id=req.task_id, result=f"ERROR: {str(e)}", node_id=self.id)

    task_executor = ThreadPoolExecutor(max_workers=256)

    def assign_task(self, req):
        self.weight = int(self.weight)
        
        log(f"Assigning task {req.task_id} (type: {req.task_type})", node=self.id, lvl=logging.DEBUG)
        
        if self.mode=="replication":
            targets=self.choose_targets(req, 2)
        else:
            targets=[self.choose_targets(req, 1)[0]]
        
        log(f"Selected targets for task {req.task_id}: {targets}", node=self.id, lvl=logging.DEBUG)
        
        target = targets[0]
        
        if target == self.id:
            self.task_queue.append(req)
            log(f"Added task {req.task_id} to queue, queue length: {len(self.task_queue)}", node=self.id, lvl=logging.DEBUG)
            
            return self.exec_task(req)
        else:
            future = self.task_executor.submit(self.run_on_target, req, target)
            return future.result()

    task_distribution_counter = 0
    
    # where actual task allocation happens -> in if case - round robin ,  when --advanced_weights param is used,  in else case task allocation based on weight happens
    def choose_targets(self, req, k):
        self.weight = int(self.weight)
        
        if not hasattr(self, 'advanced_weight') or not self.advanced_weight:
            all_nodes = sorted(set(list(self.peers.keys()) + [self.id]))
            num_nodes = len(all_nodes)
            
            task_id = getattr(req, 'task_id', 0)
            try:
                task_id_int = int(task_id)
            except (ValueError, TypeError):
                task_id_int = 0
            
          
            
            Node.task_distribution_counter = (Node.task_distribution_counter + 1) % 1000000
            
            distribution_key = Node.task_distribution_counter % num_nodes
            
            result = []
            for i in range(k):
                idx = (distribution_key + i) % num_nodes
                result.append(all_nodes[idx])
            
            log(f"Task ID: {task_id_int}, Selected nodes: {result} (using counter {Node.task_distribution_counter})", 
                node=self.id, lvl=logging.INFO)
            
            return result
        else:
            # Advanced target selection based on multiple factors
            scores = {}
            task_type = getattr(req, 'task_type', 'default')
            task_id = getattr(req, 'task_id', 0)
            
            for node_id in list(self.peers.keys()) + [self.id]:
                if node_id == self.id:
                    score = self.weight
                    
                    queue_length = len(self.task_queue)
                    score *= (1.0 / (1.0 + 4.0 * queue_length))  # Increased penalty factor  to 4.0 for queue length
                    
                    if self.task_type_attempts.get(task_type, 0) > 0:
                        success_rate = self.task_type_success.get(task_type, 0) / self.task_type_attempts[task_type]
                        score *= (1.0 + 0.2 * success_rate)
                else:
                    if node_id in self.peer_stats:
                        stats = self.peer_stats[node_id]
                        score = stats.weight
                        
                        score *= (1.0 / (1.0 + 1.0 * stats.queue_length)) 
                        
                        if task_type in stats.specialization:
                            score *= (1.0 + 0.2 * stats.specialization[task_type])
                    else:
                        score = max(0, 10-node_id)
                
                if node_id in self.network_distances:
                    distance = self.network_distances[node_id]
                    score *= (1.0 / (1.0 + 0.1 * distance))
                
               
                
                if node_id == self.id:
                    log(f"Node {node_id} score: {score}, queue length: {len(self.task_queue)}", 
                        node=self.id, lvl=logging.DEBUG)
                else:
                    log(f"Node {node_id} score: {score}, queue length: {self.peer_stats[node_id].queue_length if node_id in self.peer_stats else 'unknown'}", 
                        node=self.id, lvl=logging.DEBUG)
                
                scores[node_id] = score
            
            result = sorted(scores.keys(), key=lambda nid: -scores[nid])[:k]
            
            log(f"Task {task_id} assigned to nodes {result} based on scores: {[(nid, scores[nid]) for nid in result]}", 
                node=self.id, lvl=logging.INFO)
                
            return result

    forward_executor = ThreadPoolExecutor(max_workers=100)
    
    active_forwards = {}
    
    def run_on_target(self, req, tid):
        self.weight = int(self.weight)
        
        if tid==self.id: return self.exec_task(req)
        
        forward_id = f"forward-{self.id}-{tid}-{req.task_id}-{time.time()}-{random.randint(0, 1000000)}"
        
        def forward_task(forward_id, target_id, request):
            try:
                task_type = getattr(request, 'task_type', 'default')
                result = self.stubs[target_id].ExecuteTask(
                    pb.TaskAssignment(
                        task_id=request.task_id,
                        payload=request.payload,
                        task_type=task_type
                    ),
                    timeout=5
                )
                
                with Node.task_lock:
                    if forward_id in Node.active_forwards:
                        del Node.active_forwards[forward_id]
                
                return result
            except grpc.RpcError:
                with Node.task_lock:
                    if forward_id in Node.active_forwards:
                        del Node.active_forwards[forward_id]
                
                return pb.TaskResult(task_id=request.task_id, result="", node_id=target_id)
        
        future = self.forward_executor.submit(forward_task, forward_id, tid, req)
        
        with Node.task_lock:
            Node.active_forwards[forward_id] = future
        
        return future.result()

    perform_executor = ThreadPoolExecutor(max_workers=100)  
    
    task_lock = threading.Lock()
    
    active_tasks = {}
    
    def perform(self, payload):
        # Simulate CPU/IO work by blocking for `payload` milliseconds so the task
        # actually occupies a worker thread for its full duration. This makes
        # throughput/latency measurements reflect real per-task work and real
        # cross-node concurrency, instead of just gRPC dispatch overhead.
        # (Previously this submitted the sleep to a detached thread pool and
        #  returned "done" immediately, so the payload duration had no effect.)
        try: dur = float(payload) / 1000.0
        except: dur = 0.1
        time.sleep(dur)
        return "done"

    # election / heartbeat loops 
    def start_election(self):
        self.term += 1
        self.is_leader = False
        self.leader_id = None
        self.weight = int(self.weight)

        log(f"Election start (term {self.term}, weight {self.weight})",
            node=self.id, lvl=logging.INFO)

        higher_exists = False

        # Check all peers for higher priority nodes
        for pid, addr in self.peers.items():
            if pid == self.id:  
                continue
            try:
               
                with grpc.insecure_channel(addr,
                                           options=[('grpc.keepalive_timeout_ms', 500)]) as ch:
                    stub = pb_grpc.DwseNodeStub(ch)
                    resp = stub.Election(
                        pb.ElectionRequest(candidate_id=self.id,
                                           candidate_weight=self.weight),
                        timeout=0.3)
                log(f"Node {pid} replied ok={resp.ok}, "
                    f"w={resp.responder_weight}",
                    node=self.id, lvl=logging.DEBUG)
                if resp.ok:
                    higher_exists = True
                    log(f"Node {pid} outranks me → abort election",
                        node=self.id, lvl=logging.INFO)
                    break
            except grpc.RpcError as e:
                log(f"Election RPC to {pid} failed: {e.code().name}",
                    node=self.id, lvl=logging.WARNING)
            except Exception as e:
                log(f"Error contacting {pid}: {e}",
                    node=self.id, lvl=logging.WARNING)

        if not higher_exists:
            log("No higher node responded → I become leader",
                node=self.id, lvl=logging.INFO)
            self.become_leader()

    def become_leader(self):
        self.weight = int(self.weight)
        self.is_leader = True
        self.leader_id = self.id
        log(f"Became leader (term {self.term}, weight {self.weight})",
            node=self.id, lvl=logging.INFO)

        node_stats = pb.NodeStats(
            node_id=self.id,
            queue_length=len(self.task_queue),
            cpu_usage=self.get_cpu_usage(),
            memory_available=self.get_available_memory(),
            avg_response_time=self.avg_response_time,
            tasks_completed=self.tasks_completed,
            tasks_failed=self.tasks_failed,
            specialization={t: float(s/self.task_type_attempts[t]) 
                           for t, s in self.task_type_success.items() 
                           if self.task_type_attempts[t] > 0}
        )

        for pid, addr in self.peers.items():
           if pid == self.id: 
               continue
           try:
               with grpc.insecure_channel(addr,
                                          options=[('grpc.keepalive_timeout_ms', 500)]) as ch:
                   stub = pb_grpc.DwseNodeStub(ch)
                   stub.Heartbeat(
                       pb.HeartbeatRequest(
                           leader_id=self.id,
                           term=self.term,
                           leader_weight=self.weight,
                           stats=node_stats
                       ),
                       timeout=0.3
                   )
               log(f"Sent initial heartbeat to node {pid} as new leader",
                   node=self.id, lvl=logging.INFO)
           except grpc.RpcError as e:
               log(f"Error sending initial heartbeat to node {pid}: {e.code().name}",
                   node=self.id, lvl=logging.WARNING)
           except Exception as e:
               log(f"Error contacting {pid}: {e}",
                   node=self.id, lvl=logging.WARNING)

    def hb_loop(self):
        while True:
            if self.is_leader:
                self.weight = int(self.weight)
                
                node_stats = pb.NodeStats(
                    node_id=self.id,
                    queue_length=len(self.task_queue),
                    cpu_usage=self.get_cpu_usage(),
                    memory_available=self.get_available_memory(),
                    avg_response_time=self.avg_response_time,
                    tasks_completed=self.tasks_completed,
                    tasks_failed=self.tasks_failed,
                    specialization={t: float(s/self.task_type_attempts[t]) 
                                   for t, s in self.task_type_success.items() 
                                   if self.task_type_attempts[t] > 0}
                )
                
                for pid, stub in self.stubs.items():
                    try: 
                        log(f"hb→{pid}", node=self.id, lvl=logging.DEBUG)
                        response = stub.Heartbeat(
                            pb.HeartbeatRequest(
                                leader_id=self.id,
                                term=self.term,
                                leader_weight=self.weight,
                                stats=node_stats
                            ),
                            timeout=0.3
                        )
                        
                        if hasattr(response, 'stats') and response.stats:
                            stats = response.stats
                            self.peer_stats[pid] = PeerStats(
                                weight=response.follower_weight,
                                queue_length=stats.queue_length,
                                specialization={k: v for k, v in stats.specialization.items()}
                            )
                            log(f"Updated stats for node {pid}: weight={response.follower_weight}, queue_length={stats.queue_length}", 
                                node=self.id, lvl=logging.DEBUG)
                    except grpc.RpcError: pass
            time.sleep(1)

    def monitor_loop(self):
       # start election if leader heartbeat absent
        while True:
            self.weight = int(self.weight)
            if not self.is_leader and time.time() - self.last_hb > 3:
                log("Heartbeat timeout → election",
                    node=self.id, lvl=logging.WARNING)
                self.start_election()
            time.sleep(0.5)


    def serve(self):
        self.weight = int(self.weight)
        
        self.server.start()
        log(f"Listening on {self.addr}",node=self.id)
        if self.id==0: self.become_leader()
        threading.Thread(target=self.hb_loop,daemon=True).start()
        threading.Thread(target=self.monitor_loop,daemon=True).start()
        
        if self.enable_stealing:
            threading.Thread(target=self.stealing_loop,daemon=True).start()
            
        self.server.wait_for_termination()
        
    def stealing_loop(self):
        #Background thread that attempts to steal work 
        while True:
            self.weight = int(self.weight)
            
            if not self.is_leader and self.enable_stealing: 
                log(f"Attempting to steal work, queue length: {len(self.task_queue)}", node=self.id, lvl=logging.DEBUG)
                result = self.attempt_steal_work()
                if result:
                    log(f"Successfully stole work", node=self.id, lvl=logging.INFO)
                else:
                    log(f"Failed to steal work", node=self.id, lvl=logging.DEBUG)
            time.sleep(0.5)  

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--id",type=int,required=True)
    p.add_argument("--port",type=int,required=True)
    p.add_argument("--peers",required=True)
    p.add_argument("--mode",choices=["replication","consistency"],default="consistency")
    p.add_argument("--log-level",choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"],default="INFO")
    p.add_argument("--advanced-weight",action="store_true",help="Use advanced weight calculation")
    p.add_argument("--enable-stealing",action="store_true",help="Enable work stealing")
    a=p.parse_args()

  
    setup_logging(a.log_level)

    peers=parse_peers(a.peers); peers[a.id]=f"localhost:{a.port}"
    Node(a.id, peers[a.id], peers, a.mode, 
         advanced_weight=a.advanced_weight, 
         enable_stealing=a.enable_stealing).serve()
