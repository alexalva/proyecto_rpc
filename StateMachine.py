class StateMachine:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key, None)

    def set(self, key, value):
        self.data[key] = value
        return True

    def add(self, key, value):
        if key in self.data and isinstance(self.data[key], (int, float)) and isinstance(value, (int, float)):
            self.data[key] += value
            return True
        else:
            return False

    def mult(self, key, value):
        if key in self.data and isinstance(self.data[key], (int, float)) and isinstance(value, (int, float)):
            self.data[key] *= value
            return True
        else:
            return False
