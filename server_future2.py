# Servidor
import rpyc
from rpyc.utils.server import ThreadedServer
from StateMachine_future2 import StateMachine
from operation_future import OperationFuture
import threading
import time
import socket
import json
import argparse

def leer_configuracion(archivo_config):
    with open(archivo_config, 'r') as file:
        lines = [line.strip() for line in file if line.strip() and not line.startswith('#')]
    # Asume que la primera línea es para el servidor RPyC y la segunda línea es para el servidor de socket de réplica
    rpyc_address = tuple(lines[0].split(','))  
    rpyc_address = (rpyc_address[0], int(rpyc_address[1]))
    socket_address = tuple(lines[1].split(','))  
    socket_address = (socket_address[0], int(socket_address[1]))
    # El resto de las líneas son direcciones de réplicas
    replica_addresses = [tuple(line.split(',')) for line in lines[2:]]  
    replica_addresses = [(addr[0], int(addr[1])) for addr in replica_addresses]
    return rpyc_address, socket_address, replica_addresses



class MyService(rpyc.Service):
    def __init__(self, replica_addresses, my_address):
        self.sm = StateMachine()
        self.replica_addresses = replica_addresses  # Lista de direcciones IP de las réplicas
        self.my_address = my_address  # Dirección IP y puerto de este servidor
        # Iniciar un hilo para procesar elementos del buffer continuamente
        threading.Thread(target=self.process_buffer, daemon=True).start()
        threading.Thread(target=self.init_socket_server, daemon=True).start()

    def init_socket_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.my_address)
        server_socket.listen()
        print(f"Escuchando en {self.my_address} para réplicas...")
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión entrante de {addr}")
            threading.Thread(target=self.handle_client_socket, args=(client_socket,), daemon=True).start()
    
    def handle_client_socket(self, client_socket):
        with client_socket:
            while True:
                data = client_socket.recv(1024)  # Tamaño del buffer 1024 bytes
                if not data:
                    break  # Conexión cerrada
                try:
                    message = json.loads(data.decode('utf-8'))
                    # Directamente usa los valores sin necesidad de buscar un sub-diccionario
                    operation = message.get('operation')
                    key = message.get('key')
                    value = message.get('value')
                    lamport_timestamp = message.get('timestamp', 0)
                    print(f"Recibido: Operación {operation}, clave {key}, valor {value}, timestamp {lamport_timestamp}")
                    
                    # Aquí la lógica para procesar el mensaje...
                    self.broadcast_operation(operation, key, value, self.sm.lamport_clock)
                    self.sm.lamport_clock = max(lamport_timestamp, self.sm.lamport_clock) + 1
                    print(f"Operación encolada. Tamaño del buffer ahora: {self.sm.buffer.qsize()}")
                    self.sm.produce(operation, key, value, timestamp=self.sm.lamport_clock)
                except json.JSONDecodeError as e:
                    print("Error al decodificar el mensaje JSON:", e)


    # def broadcast_operation(self, operation, key, value, timestamp):
    #     """
    #     Envía la operación a todas las réplicas y espera confirmaciones.
    #     """
    #     confirmations_received = 0
    #     total_replicas = len(self.replica_addresses)

    #     for address in self.replica_addresses:
    #         if address != self.my_address:  # No enviar a sí mismo
    #             try:
    #                 with socket.create_connection(address, timeout=10) as sock:
    #                     message = json.dumps({
    #                         "operation": operation,
    #                         "key": key,
    #                         "value": value,
    #                         "timestamp": timestamp
    #                     }).encode('utf-8')
    #                     print(f"Enviando operación a réplica en {address}: {message}")
    #                     sock.sendall(message)
    #                     # Espera por una confirmación
    #                     response = sock.recv(1024)
    #                     if response and json.loads(response.decode('utf-8')).get('confirmation', False):
    #                         confirmations_received += 1
    #             except Exception as e:
    #                 print(f"Error replicando a {address}: {e}")

    #     if confirmations_received == total_replicas - 1:
    #         # Todas las réplicas (excepto esta) han confirmado
    #         self.sm.confirm_operation(operation, timestamp)


    def broadcast_operation(self, operation, key, value, timestamp):
        confirmations_received = 0
        for address in self.replica_addresses:
            if address != self.my_address:  # No enviar a sí mismo
                try:
                    with socket.create_connection(address, timeout=10) as sock:
                        message = json.dumps({
                            "operation": operation,
                            "key": key,
                            "value": value,
                            "timestamp": timestamp
                        }).encode('utf-8')
                        print(f"Enviando operación a réplica en {address}: {message}")
                        sock.sendall(message)
                        # Espera por una confirmación
                        response = sock.recv(1024)
                        if response and json.loads(response.decode('utf-8')).get('confirmation', False):
                            print(f"Confirmación recibida de réplica en {address}")
                            confirmations_received += 1
                except Exception as e:
                    print(f"Error replicando a {address}: {e}")

        # Verificar si se recibieron todas las confirmaciones necesarias
        all_confirmed = confirmations_received == len(self.replica_addresses) - 1
        if all_confirmed:
            print(f"Todas las réplicas han confirmado la operación: {operation} con timestamp {timestamp}")
        return all_confirmed

    def replicate_to_peers(self, operation, key, value):
        message = json.dumps({
            "operation": {
                "action": operation,
                "key": key,
                "value": value
            }
        })
        for address in self.replica_addresses:
            try:
                with socket.create_connection(address, timeout=10) as sock:
                    sock.sendall(message.encode('utf-8'))
            except Exception as e:
                print(f"Error replicando a {address}: {e}")

    def exposed_read(self, key):
        # Esta operación puede permanecer igual, ya que get no necesita interactuar con el buffer
        return self.sm.get(key)

    def exposed_update(self, key, value, operation):
        # Incrementa el timestamp de Lamport antes de enviar la operación
        timestamp = self.sm.increment_clock()
        print(f"Iniciando broadcast para la operación: {operation} con timestamp: {timestamp}")

        # Realiza el broadcast de la operación a todas las réplicas
        all_confirmed = self.broadcast_operation(operation, key, value, timestamp)

        if all_confirmed:
            print(f"Operación {operation} confirmada por todas las réplicas.")
            # Procede solo si todas las réplicas han confirmado la operación
            result = self.sm.produce(operation, key, value, timestamp=timestamp)
            # Aquí podrías aplicar la operación de manera efectiva o encolarla para su procesamiento
            print(f"Operación {operation} aplicada con éxito.")
            return True
        else:
            print(f"Operación {operation} no pudo ser confirmada por todas las réplicas.")
            return False



    def process_buffer(self):
        # Hilo que consume operaciones del buffer y las aplica
        while True:
            op = self.sm.consume()
            if op is not None:
                # Asegúrate de que op tenga la longitud esperada antes de desempaquetar
                if len(op) == 4:
                    operation, key, value, future = op
                    print(f"Consumiendo operación: {operation} {key}={value}")
                    # Simular un tiempo de procesamiento
                    time.sleep(2)  # Espera de 2 segundos para ilustrar el consumo lento

                    try:
                        # Realiza la operación y establece el resultado en el futuro
                        if operation == 'set':
                            result = self.sm.set(key, value)
                        elif operation == 'add':
                            result = self.sm.add(key, value)
                        elif operation == 'mult':
                            result = self.sm.mult(key, value)
                        elif operation == 'get':
                            result = self.sm.get(key)
                        else:
                            raise ValueError("Operación desconocida")

                        # Si la operación tiene un futuro asociado, establece el resultado
                        if future is not None:
                            future.set_result(result)
                    
                    except Exception as e:
                        # Si ocurre un error durante la operación, establece la excepción en el futuro
                        if future is not None:
                            future.set_exception(e)

                else:
                    print("Operación consumida no tiene el formato esperado.")
            else:
                # Manejar el caso en el que consume devuelva None (por ejemplo, esperando un poco antes de reintentar)
                time.sleep(1)


    def exposed_get(self, key):
        future = OperationFuture()
        self.sm.produce('get', key, None, future)
        return future.get_result()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Servidor RPyC con configuración de réplicas.')
    parser.add_argument('config_file', type=str, help='Archivo de configuración del servidor.')
    args = parser.parse_args()

    # Leer configuración del servidor
    rpyc_address, socket_address, replica_addresses = leer_configuracion(args.config_file)
    
    # Crear instancia del servicio con la configuración leída
    service = MyService(replica_addresses, socket_address)
    
    # Iniciar el servidor RPyC con la instancia de servicio y el puerto correcto
    t = ThreadedServer(service, port=rpyc_address[1])
    t.start()

