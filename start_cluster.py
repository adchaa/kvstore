import threading
import time
import sys
from kv_node import KVStoreNode
from coordinator import Coordinator

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
    try:
        coordinator = start_coordinator()
        time.sleep(1)
    except Exception as e:
        print(f"Failed to start coordinator: {e}")
        return

    nodes = []
    for i in range(3):
        try:
            node = start_kv_node(
                f"node_{i}", 
                'localhost', 
                6000 + i,
                coordinator
            )
            nodes.append(node)
            time.sleep(0.5)
        except Exception as e:
            print(f"Failed to start node_{i}: {e}")

    for i, node in enumerate(nodes):
        replica_target = nodes[(i + 1) % len(nodes)]
        node.add_replica({
            'node_id': replica_target.node_id,
            'host': replica_target.host,
            'port': replica_target.port
        })
        print(f"Configured {node.node_id} to replicate to {replica_target.node_id}")

    print("CLUSTER READY")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()
