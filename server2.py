# Servidor
import rpyc
from rpyc.utils.server import ThreadedServer
from StateMachine import StateMachine
import threading
import time

class MyService(rpyc.Service):
    def __init__(self):
        self.sm = StateMachine()
        # Iniciar un hilo para procesar elementos del buffer continuamente
        threading.Thread(target=self.process_buffer, daemon=True).start()

    def exposed_read(self, key):
        # Esta operación puede permanecer igual, ya que get no necesita interactuar con el buffer
        return self.sm.get(key)

    def exposed_update(self, key, value, operation):
        # En lugar de actualizar directamente, encola la operación en el buffer
        return self.sm.produce((operation, key, value))

    def process_buffer(self):
        # Hilo que consume operaciones del buffer y las aplica
        while True:
            op = self.sm.consume()
            if op is not None:
                operation, key, value = op
                print(f"Consumiendo operación: {operation} {key}={value}")
                print(f"Operación consumida. Tamaño del buffer ahora: {self.sm.buffer.qsize()}")
                # Simular un tiempo de procesamiento
                time.sleep(8)  # Espera de 2 segundos para ilustrar el consumo lento
                if operation == 'set':
                    self.sm.set(key, value)
                elif operation == 'add':
                    self.sm.add(key, value)
                elif operation == 'mult':
                    self.sm.mult(key, value)

if __name__ == "__main__":
    t = ThreadedServer(MyService, port=18812)
    t.start()
