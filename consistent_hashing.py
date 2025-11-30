import hashlib
from typing import List, Dict

class ConsistentHash:
    def __init__(self, nodes: List[str] = None, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}:{i}"
            hash_key = self._hash(virtual_node)
            self.ring[hash_key] = node
            self.sorted_keys.append(hash_key)
        
        self.sorted_keys.sort()
    
    def remove_node(self, node: str):
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}:{i}"
            hash_key = self._hash(virtual_node)
            if hash_key in self.ring:
                del self.ring[hash_key]
                self.sorted_keys.remove(hash_key)
    
    def get_node(self, key: str) -> str:
        if not self.ring:
            return None
        
        hash_key = self._hash(key)
        
        for ring_key in self.sorted_keys:
            if ring_key >= hash_key:
                return self.ring[ring_key]
        
        return self.ring[self.sorted_keys[0]]

    def get_nodes(self, key: str, count: int = 1) -> List[str]:
        if not self.ring:
            return []
        
        hash_key = self._hash(key)
        nodes = []
        start_index = 0
        found = False
        for i, ring_key in enumerate(self.sorted_keys):
            if ring_key >= hash_key:
                start_index = i
                found = True
                break
        
        if not found:
            start_index = 0
        seen_nodes = set()
        total_keys = len(self.sorted_keys)
        
        for i in range(total_keys):
            idx = (start_index + i) % total_keys
            ring_key = self.sorted_keys[idx]
            node = self.ring[ring_key]
            
            if node not in seen_nodes:
                nodes.append(node)
                seen_nodes.add(node)
                
            if len(nodes) == count:
                break
                
        return nodes