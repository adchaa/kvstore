import unittest
import threading
import time
import sys
import os
import socket
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kv_node import KVStoreNode
from coordinator import Coordinator
from client import KVClient

class TestFailover(unittest.TestCase):
    def setUp(self):
        self.coordinator_host = 'localhost'
        self.coordinator_port = 5002
        self.nodes = []
        self.coordinator = None
        
    def tearDown(self):
        if self.coordinator:
            self.coordinator.running = False
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.coordinator_host, self.coordinator_port))
                    s.send(json.dumps({'operation': 'HEALTH'}).encode())
            except:
                pass

        for node in self.nodes:
            node.running = False
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((node.host, node.port))
                    s.send(json.dumps({'operation': 'HEALTH'}).encode())
            except:
                pass

    def start_coordinator(self):
        coordinator = Coordinator(self.coordinator_host, self.coordinator_port)
        coordinator_thread = threading.Thread(target=coordinator.start_server)
        coordinator_thread.daemon = True
        coordinator_thread.start()
        return coordinator

    def start_kv_node(self, node_id, host, port, coordinator):
        node = KVStoreNode(node_id, host, port, 
                          coordinator_host=coordinator.host, 
                          coordinator_port=coordinator.port)
        node_thread = threading.Thread(target=node.start_server)
        node_thread.daemon = True
        node_thread.start()
        return node

    def test_failover(self):
        self.coordinator = self.start_coordinator()
        time.sleep(1)
        
        for i in range(3):
            node = self.start_kv_node(
                f"node_{i}", 
                'localhost', 
                6010 + i,
                self.coordinator
            )
            self.nodes.append(node)
            time.sleep(0.5)
        
        client = KVClient('localhost', self.coordinator_port)
        time.sleep(1)
        
        key = "failover_key"
        target_nodes = self.coordinator.consistent_hash.get_nodes(key, count=2)
        
        self.assertTrue(len(target_nodes) >= 2, "Need at least 2 nodes for failover test")
        
        primary_id = target_nodes[0]
        secondary_id = target_nodes[1]
        
        primary_node = next(n for n in self.nodes if n.node_id == primary_id)
        secondary_node = next(n for n in self.nodes if n.node_id == secondary_id)
        primary_node.add_replica({
            'node_id': secondary_node.node_id,
            'host': secondary_node.host,
            'port': secondary_node.port
        })
        
        value = "failover_value"
        success = client.set(key, value)
        self.assertTrue(success, "SET failed")
        
        time.sleep(0.5)
        self.assertEqual(secondary_node.get(key), value, "Replication failed")
        
        primary_node.running = False
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((primary_node.host, primary_node.port))
                s.send(json.dumps({'operation': 'HEALTH'}).encode())
        except:
            pass
        time.sleep(1)
        
        retrieved_value = client.get(key)
        
        self.assertEqual(retrieved_value, value, "Failover retrieval failed")

if __name__ == "__main__":
    unittest.main()
