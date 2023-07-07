class Person:
    def __init__(self, name, age, gender, email, phone, address, city, state, country, salary):
        self.name = name
        self.age = age
        self.gender = gender
        self.email = email
        self.phone = phone
        self.address = address
        self.city = city
        self.state = state
        self.country = country
        self.salary = salary

class DatabaseConfig:
    def __init__(self):
        self.account = None
        self.user = None
        self.password = None
        self.role = None
        self.database = None
        self.warehouse = None
        self.schema = None
