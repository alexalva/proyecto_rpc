import rpyc

def main():
    # Conexión al servidor modificando el puerto si es necesario y el IP del servidor
    # conn = rpyc.connect("192.168.1.10", 18812)
    conn = rpyc.connect("localhost", 18812)
    while True:
        # Muestra opciones al usuario
        print("\nOpciones:")
        print("1. Leer valor (read)")
        print("2. Actualizar valor (update)")
        print("3. Salir")
        choice = input("Elige una opción (1-3): ")

        if choice == '1':
            # Leer valor
            key = input("Ingresa la clave para leer: ")
            print("Read:", conn.root.exposed_read(key))

        elif choice == '2':
            # Actualizar valor
            key = input("Ingresa la clave para actualizar: ")
            value = input("Ingresa el valor: ")
            operation = input("Elige la operación (set, add, mult): ")
            if operation in ['set', 'add', 'mult']:
                # Convertir valor a número si es posible
                try:
                    value = float(value) if '.' in value else int(value)
                except ValueError:
                    print("Por favor ingresa un número válido.")
                    continue
                result = conn.root.exposed_update(key, value, operation)
                print("Resultado de la actualización:", "Exitoso" if result else "Fallido")
            else:
                print("Operación no válida.")

        elif choice == '3':
            # Salir del programa
            print("Saliendo del programa.")
            break

        else:
            print("Opción no válida.")

if __name__ == "__main__":
    main()
