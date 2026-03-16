from pydantic import BaseModel, ValidationError

class Person(BaseModel):
    name: str
    age: int

# Valid data
p1 = Person(name="Alice", age=25)
print(p1)

# Invalid data
try:
    p2 = Person(name="Bob", age="twenty")
except ValidationError as e:
    print(e)