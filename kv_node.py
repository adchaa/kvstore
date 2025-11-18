import threading
import socket
import json
import time
from typing import Dict, Any, Optional
from consistent_hashing import ConsistentHash

class KVStoreNode:
    def __init__(self, node_id: str, host: str, port: int, replica_of: str = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data: Dict[str, Any] = {}
        self.locks: Dict[str, threading.Lock] = {}
        self.replica_of = replica_of  # For master-replica setup
        self.replicas = []  # List of replica nodes
        self.running = False
        
    def get_lock(self, key: str) -> threading.Lock:
        """Get or create a lock for a specific key"""
        if key not in self.locks:
            self.locks[key] = threading.Lock()
        return self.locks[key]
    
    def set(self, key: str, value: Any, sync_replicas: bool = True) -> bool:
        """Store a key-value pair"""
        with self.get_lock(key):
            self.data[key] = {
                'value': value,
                'timestamp': time.time(),
                'version': self.data.get(key, {}).get('version', 0) + 1
            }
            
            # Sync to replicas if this is master
            if sync_replicas and not self.replica_of:
                self._sync_to_replicas('SET', key, value)
                
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key"""
        with self.get_lock(key):
            if key in self.data:
                return self.data[key]['value']
            return None
    
    def delete(self, key: str, sync_replicas: bool = True) -> bool:
        """Delete a key-value pair"""
        with self.get_lock(key):
            if key in self.data:
                del self.data[key]
                
                # Sync to replicas if this is master
                if sync_replicas and not self.replica_of:
                    self._sync_to_replicas('DELETE', key)
                    
                return True
            return False
    
    def _sync_to_replicas(self, operation: str, key: str, value: Any = None):
        """Sync operations to replica nodes"""
        for replica in self.replicas:
            try:
                self._send_to_node(replica, {
                    'operation': operation,
                    'key': key,
                    'value': value,
                    'sync': True  # Mark as sync operation
                })
            except Exception as e:
                print(f"Failed to sync to replica {replica}: {e}")
    
    def _send_to_node(self, node_info: dict, message: dict):
        """Send message to another node"""
        # Implementation for inter-node communication
        pass
    
    def add_replica(self, replica_node: dict):
        """Add a replica node"""
        self.replicas.append(replica_node)
    
    def start_server(self):
        """Start the node server to handle requests"""
        self.running = True
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)
        
        print(f"KV Node {self.node_id} listening on {self.host}:{self.port}")
        
        while self.running:
            try:
                client_socket, addr = server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, addr)
                )
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket: socket.socket, addr: tuple):
        """Handle client requests"""
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if data:
                request = json.loads(data)
                response = self._process_request(request)
                client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            client_socket.close()
    
    def _process_request(self, request: dict) -> dict:
        """Process incoming requests"""
        operation = request.get('operation')
        key = request.get('key')
        value = request.get('value')
        
        try:
            if operation == 'SET':
                success = self.set(key, value, sync_replicas=not request.get('sync', False))
                return {'success': success, 'operation': 'SET'}
            
            elif operation == 'GET':
                result = self.get(key)
                return {'success': result is not None, 'value': result}
            
            elif operation == 'DELETE':
                success = self.delete(key, sync_replicas=not request.get('sync', False))
                return {'success': success, 'operation': 'DELETE'}
            
            elif operation == 'HEALTH':
                return {'status': 'healthy', 'node_id': self.node_id, 'data_size': len(self.data)}
            
            else:
                return {'success': False, 'error': f'Unknown operation: {operation}'}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}