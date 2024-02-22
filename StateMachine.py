import threading
import queue

class StateMachine:
    def __init__(self):
        self.data = {}
        self.buffer = queue.Queue(maxsize=7)  # Buffer de tamaño 7
        self.semaphore = threading.Semaphore(value=1)  # Controla el acceso al recurso compartido

    def get(self, key):
        with self.semaphore:  # Asegura el acceso exclusivo para leer
            return self.data.get(key, None)

    def set(self, key, value):
        with self.semaphore:  # Asegura el acceso exclusivo para modificar
            self.data[key] = value
            return True

    def add(self, key, value):
        with self.semaphore:  # Asegura el acceso exclusivo para modificar
            if key in self.data and isinstance(self.data[key], (int, float)) and isinstance(value, (int, float)):
                self.data[key] += value
                return True
            else:
                return False

    def mult(self, key, value):
        with self.semaphore:  # Asegura el acceso exclusivo para modificar
            if key in self.data and isinstance(self.data[key], (int, float)) and isinstance(value, (int, float)):
                self.data[key] *= value
                return True
            else:
                return False
        
    def produce(self, item):
        try:
            self.buffer.put(item, block=True, timeout=5)
            print(f"Operación encolada. Tamaño del buffer ahora: {self.buffer.qsize()}")
            return True
        except queue.Full:
            print("Buffer lleno: el productor no puede añadir más elementos.")
            return False


    def consume(self):
        try:
            item = self.buffer.get(block=True, timeout=5)  # Intenta retirar un elemento del buffer, esperando hasta 5 segundos si está vacío
            self.buffer.task_done()
            return item
        except queue.Empty:
            print("Buffer vacío: el consumidor no puede retirar elementos.")
            return None
