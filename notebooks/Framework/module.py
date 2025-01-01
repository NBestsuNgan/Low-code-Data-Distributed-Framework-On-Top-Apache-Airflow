from types import ModuleType

class Framework(ModuleType):
    def greet(self):
        print("Hello there")