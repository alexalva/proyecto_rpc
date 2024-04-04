import threading
import queue

class StateMachine:
    def __init__(self):
        self.data = {"clave1": 0, "clave2": 0, "clave3": 0}
        self.buffer = queue.PriorityQueue()  # Buffer de tamaño 7
        self.semaphore = threading.Semaphore(value=1)
        self.lamport_clock = 0  # Inicializa el reloj de Lamport  # Controla el acceso al recurso compartido
    
    def peek(self):
        with self.semaphore:
            try:
                # Intenta obtener el elemento superior sin bloquear
                item = self.buffer.get(block=False)
                # Inmediatamente reinserta el item para simular un 'peek'
                self.buffer.put(item)
                return item
            except queue.Empty:
                # Retorna None si la cola está vacía
                return None
            except Exception as e:
                print(f"Error al intentar realizar peek en el buffer: {e}")
                return None

    def increment_clock(self):
        with self.semaphore:
            self.lamport_clock += 1
            return self.lamport_clock

    def update_clock(self, timestamp):
        with self.semaphore:
            self.lamport_clock = max(self.lamport_clock, timestamp) + 1
            return self.lamport_clock

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
        
    def produce(self, operation, key, value=None, future=None, timestamp=None):
        if operation == 'get':
            # Para operaciones de lectura, simplemente devuelve el valor actual sin encolar
            # Esto asume que las operaciones 'get' no necesitan ser secuencializadas con operaciones de escritura
            # Si necesitas secuencializar 'get' con otras operaciones, ajusta esta lógica según sea necesario
            with self.semaphore:
                return self.data.get(key, None)
        else:
            # Utiliza el timestamp proporcionado, si está presente; de lo contrario, incrementa el reloj de Lamport localmente.
            if timestamp is None:
                timestamp = self.increment_clock()
            else:
                # Asegúrate de actualizar el reloj de Lamport con el timestamp recibido para mantener la coherencia.
                self.update_clock(timestamp)

            item = (timestamp, operation, key, value, future)
            try:
                self.buffer.put(item, block=True, timeout=5)
                print(f"Operación {operation} encolada para la clave {key} con timestamp {timestamp}.")
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
        