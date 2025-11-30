import threading
import socket
import json
from typing import List, Dict, Any
from consistent_hashing import ConsistentHash

class Coordinator:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.nodes: Dict[str, Dict] = {} 
        self.consistent_hash = ConsistentHash()
        self.running = False
    
    def register_node(self, node_id: str, host: str, port: int):
        self.nodes[node_id] = {
            'host': host,
            'port': port,
            'node_id': node_id
        }
        self.consistent_hash.add_node(node_id)
    
    def unregister_node(self, node_id: str):
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.consistent_hash.remove_node(node_id)
    
    def get_node_for_key(self, key: str) -> Dict[str, Any]:
        node_id = self.consistent_hash.get_node(key)
        return self.nodes.get(node_id)
    
    def route_request(self, key: str, operation: str, value: Any = None) -> Dict[str, Any]:
        target_nodes = self.consistent_hash.get_nodes(key, count=2)
        
        if not target_nodes:
            return {'success': False, 'error': 'No available nodes'}
        
        last_error = None
        
        for node_id in target_nodes:
            node_info = self.nodes.get(node_id)
            if not node_info:
                continue
            
            try:
                return self._send_to_node(node_info, {
                    'operation': operation,
                    'key': key,
                    'value': value
                })
            except Exception as e:
                print(f"Node {node_id} failed: {e}, trying next node...")
                last_error = e
                continue
            
        return {'success': False, 'error': f'All nodes failed. Last error: {str(last_error)}'}
    
    def _send_to_node(self, node_info: Dict, message: Dict) -> Dict:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((node_info['host'], node_info['port']))
                sock.send(json.dumps(message).encode('utf-8'))
                
                response = sock.recv(1024 * 1024).decode('utf-8')
                return json.loads(response)
        except Exception as e:
            raise Exception(f"Failed to communicate with node {node_info['node_id']}: {e}")
    
    def start_server(self):
        self.running = True
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)
        
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
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if data:
                request = json.loads(data)
                response = self._process_client_request(request)
                client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            client_socket.close()
    
    def _process_client_request(self, request: Dict) -> Dict:
        operation = request.get('operation')
        key = request.get('key')
        
        if operation in ['SET', 'GET', 'DELETE']:
            return self.route_request(key, operation, request.get('value'))
        elif operation == 'HEALTH':
            return {
                'status': 'healthy',
                'node_count': len(self.nodes),
                'nodes': list(self.nodes.keys())
            }
        elif operation == 'REGISTER':
            node_id = request.get('node_id')
            host = request.get('host')
            port = request.get('port')
            if node_id and host and port:
                self.register_node(node_id, host, port)
                return {'success': True, 'message': f'Node {node_id} registered'}
            else:
                return {'success': False, 'error': 'Missing registration details'}
        else:
            return {'success': False, 'error': f'Unknown operation: {operation}'}