import unittest
import threading
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kv_node import KVStoreNode
from coordinator import Coordinator
from client import KVClient

class TestKVSystem(unittest.TestCase):
    def setUp(self):
        self.coordinator_host = 'localhost'
        self.coordinator_port = 5000
        self.nodes = []
        self.coordinator = None
        
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

    def test_basic_operations(self):
        self.coordinator = self.start_coordinator()
        time.sleep(1)
        
        for i in range(3):
            node = self.start_kv_node(
                f"node_{i}", 
                'localhost', 
                6000 + i,
                self.coordinator
            )
            self.nodes.append(node)
            time.sleep(0.5)
        
        for i, node in enumerate(self.nodes):
            replica_target = self.nodes[(i + 1) % len(self.nodes)]
            node.add_replica({
                'node_id': replica_target.node_id,
                'host': replica_target.host,
                'port': replica_target.port
            })
        
        client = KVClient('localhost', 5000)
        
        time.sleep(2)
        
        test_data = {
            'user:1': {'name': 'Alice', 'age': 30},
            'user:2': {'name': 'Bob', 'age': 25},
            'product:1': {'name': 'Laptop', 'price': 999},
            'product:2': {'name': 'Mouse', 'price': 25},
        }
        
        for key, value in test_data.items():
            success = client.set(key, value)
            self.assertTrue(success, f"SET failed for key {key}")
        
        for key, expected_value in test_data.items():
            value = client.get(key)
            self.assertEqual(value, expected_value, f"GET failed for key {key}")
        
        health = client.health()
        self.assertIsNotNone(health)

if __name__ == "__main__":
    unittest.main()
