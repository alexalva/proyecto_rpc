import threading
import queue

class StateMachine:
    def __init__(self):
        self.data = {}
        self.buffer = queue.PriorityQueue(maxsize=7)  # Buffer de tamaño 7 PriorityQueue para ordenar por marcas de tiempo
        self.semaphore = threading.Semaphore(value=1)  # Controla el acceso al recurso compartido
        self.lamport_clock = 0
        self.waiting = []
        self.pending_operations = []

    def increment_clock(self):
        self.lamport_clock += 1
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
        
    # def produce(self, operation, key, value=None, future=None):
    # # Asegúrate de incluir el futuro como parte del ítem encolado
    #     item = (operation, key, value, future)
    #     try:
    #         self.buffer.put(item, block=True, timeout=5)
    #         print(f"Operación {operation} encolada para la clave {key}.")
    #         return True
    #     except queue.Full:
    #         print("Buffer lleno: el productor no puede añadir más elementos.")
    #         return False
        
    def produce(self, operation, key, value=None, future=None, timestamp=None):
        self.semaphore.acquire()
        timestamp = timestamp or self.increment_clock()
        item = (timestamp, operation, key, value, future)
        # self.buffer.put(item)
        self.pending_operations.append((self.lamport_clock, operation, key, value, future))
        self.semaphore.release()
        print(f"Operación {operation} encolada para la clave {key} con timestamp {timestamp}.")

    def confirm_operation(self, operation, timestamp):
        """
        Confirma la operación basada en su marca de tiempo y procede a ejecutarla
        si todas las confirmaciones han sido recibidas.
        """
        # Aquí verificarías si todas las réplicas han confirmado la operación
        # basándote en el timestamp y luego procederías a ejecutarla.
        # Para este ejemplo, asumimos que ya se han recibido todas las confirmaciones
        # y procedemos directamente a ejecutar la operación.
        self.process_confirmed_operation(operation)

    def process_confirmed_operation(self, operation, key, value, future):
        # Incrementa el reloj de Lamport para reflejar que se está procesando una nueva operación
        self.adjust_lamport_clock(0)

        # Ejecuta la operación basada en el tipo de acción
        if operation == 'set':
            result = self.set(key, value)
        elif operation == 'add':
            result = self.add(key, value)
        elif operation == 'mult':
            result = self.mult(key, value)
        elif operation == 'get':
            result = self.get(key)
        else:
            result = None
            print(f"Operación desconocida: {operation}")

        # Si la operación tiene un futuro asociado (indicando que fue iniciada por una solicitud que espera una respuesta),
        # se establece el resultado o la excepción correspondiente en ese futuro.
        if future is not None:
            if result is not None:
                future.set_result(result)
            else:
                future.set_exception(Exception("Operación fallida o desconocida"))

    # Nota: Este es un esqueleto básico. Deberías adaptar el manejo de resultados y errores
    # basado en las necesidades específicas de tu aplicación y las operaciones soportadas.



    def consume(self):
        try:
            item = self.buffer.get(block=True, timeout=5)  # Intenta retirar un elemento del buffer, esperando hasta 5 segundos si está vacío
            self.buffer.task_done()
            return item
        except queue.Empty:
            print("Buffer vacío: el consumidor no puede retirar elementos.")
            return None
        