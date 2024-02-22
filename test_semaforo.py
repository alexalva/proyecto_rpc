import threading
from StateMachine import StateMachine

def worker(state_machine, key, value, operation):
    # Esta función simula un "proceso" que realiza una operación en el recurso compartido
    result = state_machine.update_with_semaphore(key, value, operation)
    print(f"Operación: {operation} sobre clave '{key}' con valor {value} -> Resultado: {'Éxito' if result else 'Fallo'}")

def main():
    state_machine = StateMachine()
    
    # Crear y lanzar 10 hilos para simular 10 procesos accediendo al recurso compartido
    for i in range(10):
        key = f"clave{i}"
        value = i
        operation = 'set' if i % 3 == 0 else 'add' if i % 3 == 1 else 'mult'
        thread = threading.Thread(target=worker, args=(state_machine, key, value, operation))
        thread.start()

if __name__ == "__main__":
    main()
