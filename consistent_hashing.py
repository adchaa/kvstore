import hashlib
from typing import List, Dict, Any

class ConsistentHash:
    def __init__(self, nodes: List[str] = None, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str):
        """Add a node to the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}:{i}"
            hash_key = self._hash(virtual_node)
            self.ring[hash_key] = node
            self.sorted_keys.append(hash_key)
        
        self.sorted_keys.sort()
    
    def remove_node(self, node: str):
        """Remove a node from the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}:{i}"
            hash_key = self._hash(virtual_node)
            if hash_key in self.ring:
                del self.ring[hash_key]
                self.sorted_keys.remove(hash_key)
    
    def get_node(self, key: str) -> str:
        """Get the node responsible for a key"""
        if not self.ring:
            return None
        
        hash_key = self._hash(key)
        
        # Find the first node with hash >= key's hash
        for ring_key in self.sorted_keys:
            if ring_key >= hash_key:
                return self.ring[ring_key]
        
        # Wrap around to the first node
        return self.ring[self.sorted_keys[0]]