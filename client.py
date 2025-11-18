import socket
import json

class KVClient:
    def __init__(self, coordinator_host: str, coordinator_port: int):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
    
    def _send_request(self, request: dict) -> dict:
        """Send request to coordinator"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect((self.coordinator_host, self.coordinator_port))
                sock.send(json.dumps(request).encode('utf-8'))
                
                response = sock.recv(1024).decode('utf-8')
                return json.loads(response)
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def set(self, key: str, value: any) -> bool:
        """Set a key-value pair"""
        response = self._send_request({
            'operation': 'SET',
            'key': key,
            'value': value
        })
        return response.get('success', False)
    
    def get(self, key: str) -> any:
        """Get a value by key"""
        response = self._send_request({
            'operation': 'GET',
            'key': key
        })
        return response.get('value') if response.get('success') else None
    
    def delete(self, key: str) -> bool:
        """Delete a key"""
        response = self._send_request({
            'operation': 'DELETE',
            'key': key
        })
        return response.get('success', False)
    
    def health(self) -> dict:
        """Check system health"""
        return self._send_request({'operation': 'HEALTH'})