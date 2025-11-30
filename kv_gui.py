import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import json
from client import KVClient

class KVStoreGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("KV Store Client")
        self.root.geometry("600x700")
        self.client = None
        self.style = ttk.Style()
        self.style.configure("TButton", padding=6)
        self.style.configure("TLabel", padding=6)
        self._init_ui()
        self.connect_client()

    def _init_ui(self):
        conn_frame = ttk.LabelFrame(self.root, text="Connection Settings", padding=10)
        conn_frame.pack(fill="x", padx=10, pady=5)
        ttk.Label(conn_frame, text="Host:").grid(row=0, column=0, sticky="w")
        self.host_var = tk.StringVar(value="localhost")
        ttk.Entry(conn_frame, textvariable=self.host_var, width=20).grid(row=0, column=1, padx=5)
        ttk.Label(conn_frame, text="Port:").grid(row=0, column=2, sticky="w")
        self.port_var = tk.StringVar(value="5000")
        ttk.Entry(conn_frame, textvariable=self.port_var, width=10).grid(row=0, column=3, padx=5)

        ttk.Button(conn_frame, text="Connect / Reset", command=self.connect_client).grid(row=0, column=4, padx=10)
        ttk.Button(conn_frame, text="Check Health", command=self.check_health).grid(row=0, column=5, padx=5)

        ops_frame = ttk.LabelFrame(self.root, text="Operations", padding=10)
        ops_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ttk.Label(ops_frame, text="Key:").grid(row=0, column=0, sticky="w")
        self.key_var = tk.StringVar()
        ttk.Entry(ops_frame, textvariable=self.key_var, width=40).grid(row=0, column=1, columnspan=3, sticky="w", pady=5)
        
        ttk.Label(ops_frame, text="Value (JSON or String):").grid(row=1, column=0, sticky="nw", pady=5)
        self.value_text = tk.Text(ops_frame, height=8, width=40)
        self.value_text.grid(row=1, column=1, columnspan=3, pady=5, sticky="we")
        
        btn_frame = ttk.Frame(ops_frame)
        btn_frame.grid(row=2, column=0, columnspan=4, pady=10)
        
        ttk.Button(btn_frame, text="GET", command=self.do_get).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="SET", command=self.do_set).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="DELETE", command=self.do_delete).pack(side="left", padx=5)

        log_frame = ttk.LabelFrame(self.root, text="Response Log", padding=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.log_area = scrolledtext.ScrolledText(log_frame, height=10, state='disabled')
        self.log_area.pack(fill="both", expand=True)

    def log(self, message):
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, message + "\n\n")
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')

    def connect_client(self):
        try:
            host = self.host_var.get()
            port = int(self.port_var.get())
            self.client = KVClient(host, port)
            self.log(f"Client configured for {host}:{port}")
        except ValueError:
            messagebox.showerror("Error", "Port must be an integer")

    def check_health(self):
        if not self.client: return
        self.log("Checking Health...")
        try:
            res = self.client.health()
            self.log(f"HEALTH RESPONSE:\n{json.dumps(res, indent=2)}")
        except Exception as e:
            self.log(f"Error: {e}")

    def do_get(self):
        key = self.key_var.get()
        if not key:
            messagebox.showwarning("Missing Input", "Please enter a Key.")
            return
        
        self.log(f"GET '{key}'...")
        try:
            val = self.client.get(key)
            if val is None:
                self.log("Result: Key not found or operation failed.")
            else:
                self.log(f"Result:\n{json.dumps(val, indent=2) if isinstance(val, (dict, list)) else val}")
                self.value_text.delete("1.0", tk.END)
                if isinstance(val, (dict, list)):
                    self.value_text.insert("1.0", json.dumps(val, indent=2))
                else:
                    self.value_text.insert("1.0", str(val))
        except Exception as e:
            self.log(f"Error: {e}")

    def do_set(self):
        key = self.key_var.get()
        raw_value = self.value_text.get("1.0", tk.END).strip()
        
        if not key:
            messagebox.showwarning("Missing Input", "Please enter a Key.")
            return
        if not raw_value:
            messagebox.showwarning("Missing Input", "Please enter a Value.")
            return

        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
            
        self.log(f"SET '{key}' = {str(value)[:50]}...")
        try:
            success = self.client.set(key, value)
            self.log(f"Result: {'Success' if success else 'Failed'}")
        except Exception as e:
            self.log(f"Error: {e}")

    def do_delete(self):
        key = self.key_var.get()
        if not key:
            messagebox.showwarning("Missing Input", "Please enter a Key.")
            return
            
        self.log(f"DELETE '{key}'...")
        try:
            success = self.client.delete(key)
            self.log(f"Result: {'Success' if success else 'Failed'}")
        except Exception as e:
            self.log(f"Error: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = KVStoreGUI(root)
    root.mainloop()
