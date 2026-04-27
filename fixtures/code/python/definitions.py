"""
Comprehensive test cases for extracting Python definitions.
"""

# Simple function
def simple_function(x: int, y: str = "default") -> bool:
    return len(y) > x

# Generator function
def generator_function():
    yield 1
    yield 2

# Decorated function
@staticmethod
@property
def decorated_function():
    pass

# Async function definitions
async def async_function():
    pass

# Async generator definitions
async def async_generator_function():
    yield 1
    yield 2

# Decorated async function
@staticmethod
async def decorated_async_function():
    pass

# Lambda at module level
module_lambda = lambda x: x * 2

# Nested functions
def outer_function():
    def inner_function():
        pass
    
    # Nested lambda
    inner_lambda = lambda a, b: a + b
    
    return inner_function

# Simple class
class SimpleClass:
    class_var = 42

# Decorated class
from dataclasses import dataclass
@dataclass
class DecoratedClass:
    field: int

# Class methods
class ClassWithMethods:
    def method(self):
        self.attr_lambda = lambda x: x * 2
    
    async def async_method(self):
        pass

    def nested_method(self):
        def inner_method():
            pass
    
    @classmethod
    def class_method(cls):
        pass

    @classmethod
    async def async_class_method(cls):
        pass
    
    lambda_method = lambda self: self.class_var * 2

# Nested classes
class OuterClass:
    class InnerClass:
        class_var = 42