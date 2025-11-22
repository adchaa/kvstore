import threading
import time
from kv_node import KVStoreNode
from coordinator import Coordinator
from client import KVClient

def start_coordinator():
    coordinator = Coordinator('localhost', 5000)
    coordinator_thread = threading.Thread(target=coordinator.start_server)
    coordinator_thread.daemon = True
    coordinator_thread.start()
    return coordinator

def start_kv_node(node_id, host, port, coordinator):
    node = KVStoreNode(node_id, host, port, coordinator_host=coordinator.host, coordinator_port=coordinator.port)
    node_thread = threading.Thread(target=node.start_server)
    node_thread.daemon = True
    node_thread.start()
    
    return node

def main():
    print("Starting")
    
    coordinator = start_coordinator()
    time.sleep(1)
    
    nodes = []
    for i in range(3):
        node = start_kv_node(
            f"node_{i}", 
            'localhost', 
            6000 + i,
            coordinator
        )
        nodes.append(node)
        time.sleep(0.5)
    
    print("\n=== Configuring Replication ===")
    for i, node in enumerate(nodes):
        replica_target = nodes[(i + 1) % len(nodes)]
        node.add_replica({
            'node_id': replica_target.node_id,
            'host': replica_target.host,
            'port': replica_target.port
        })
        print(f"Configured {node.node_id} to replicate to {replica_target.node_id}")
    
    client = KVClient('localhost', 5000)
    
    time.sleep(2)
    
    print("\n=== Testing Basic Operations ===")
    
    test_data = {
        'user:1': {'name': 'Alice', 'age': 30},
        'user:2': {'name': 'Bob', 'age': 25},
        'product:1': {'name': 'Laptop', 'price': 999},
        'product:2': {'name': 'Mouse', 'price': 25},
    }
    
    for key, value in test_data.items():
        success = client.set(key, value)
        print(f"SET {key}: {'Success' if success else 'Failed'}")
    
    for key in test_data.keys():
        value = client.get(key)
        print(f"GET {key}: {value}")
    
    health = client.health()
    print(f"\nCluster Health: {health}")

if __name__ == "__main__":
    main()