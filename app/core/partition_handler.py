import asyncio
import time
from typing import List, Set, Optional
import logging
from enum import Enum
import aioredis

logger = logging.getLogger(__name__)

class NodeState(Enum):
    ACTIVE = "ACTIVE"
    SUSPECTED = "SUSPECTED"
    PARTITIONED = "PARTITIONED"

class PartitionHandler:
    def __init__(
        self,
        node_id: str,
        redis_client: aioredis.Redis,
        heartbeat_interval: int = 5,
        suspect_timeout: int = 15,
        partition_timeout: int = 30
    ):
        self.node_id = node_id
        self.redis = redis_client
        self.heartbeat_interval = heartbeat_interval
        self.suspect_timeout = suspect_timeout
        self.partition_timeout = partition_timeout
        
        self.known_nodes: Set[str] = set()
        self.suspected_nodes: dict = {}
        self.partitioned_nodes: dict = {}
        self.state = NodeState.ACTIVE
        self._heartbeat_task = None

    async def start(self):
        """Start the partition handler"""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Partition handler started for node {self.node_id}")

    async def stop(self):
        """Stop the partition handler"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Partition handler stopped for node {self.node_id}")

    async def _heartbeat_loop(self):
        """Maintain heartbeat with other nodes"""
        while True:
            try:
                await self._send_heartbeat()
                await self._check_nodes()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {str(e)}")
                await asyncio.sleep(self.heartbeat_interval)

    async def _send_heartbeat(self):
        """Send heartbeat to Redis"""
        timestamp = time.time()
        await self.redis.hset(
            "node_heartbeats",
            self.node_id,
            str(timestamp)
        )
        await self.redis.expire("node_heartbeats", self.partition_timeout * 2)

    async def _check_nodes(self):
        """Check the status of all nodes"""
        current_time = time.time()
        heartbeats = await self.redis.hgetall("node_heartbeats")
        
        # Convert bytes to str
        heartbeats = {k.decode(): float(v.decode()) for k, v in heartbeats.items()}
        
        # Update known nodes
        self.known_nodes.update(heartbeats.keys())
        
        for node_id in self.known_nodes:
            if node_id == self.node_id:
                continue

            last_heartbeat = heartbeats.get(node_id, 0)
            time_since_heartbeat = current_time - last_heartbeat

            if time_since_heartbeat > self.partition_timeout:
                await self._handle_partitioned_node(node_id)
            elif time_since_heartbeat > self.suspect_timeout:
                await self._handle_suspected_node(node_id)
            else:
                await self._handle_active_node(node_id)

    async def _handle_suspected_node(self, node_id: str):
        """Handle a suspected node"""
        if node_id not in self.suspected_nodes:
            self.suspected_nodes[node_id] = time.time()
            logger.warning(f"Node {node_id} is suspected of being partitioned")
            await self._notify_node_state_change(node_id, NodeState.SUSPECTED)

    async def _handle_partitioned_node(self, node_id: str):
        """Handle a partitioned node"""
        if node_id not in self.partitioned_nodes:
            self.partitioned_nodes[node_id] = time.time()
            if node_id in self.suspected_nodes:
                del self.suspected_nodes[node_id]
            logger.error(f"Node {node_id} is partitioned")
            await self._notify_node_state_change(node_id, NodeState.PARTITIONED)

    async def _handle_active_node(self, node_id: str):
        """Handle an active node"""
        if node_id in self.suspected_nodes:
            del self.suspected_nodes[node_id]
        if node_id in self.partitioned_nodes:
            del self.partitioned_nodes[node_id]
            logger.info(f"Node {node_id} has recovered from partition")
            await self._notify_node_state_change(node_id, NodeState.ACTIVE)

    async def _notify_node_state_change(self, node_id: str, state: NodeState):
        """Notify about node state changes"""
        await self.redis.publish(
            "node_state_changes",
            f"{node_id}:{state.value}"
        )

    def get_partition_status(self) -> dict:
        """Get current partition status"""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "known_nodes": list(self.known_nodes),
            "suspected_nodes": list(self.suspected_nodes.keys()),
            "partitioned_nodes": list(self.partitioned_nodes.keys())
        } 