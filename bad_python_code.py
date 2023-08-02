# A bad Python program with poor coding practices

def bad_function():
    # Poorly formatted indentation
     print("Welcome to the Bad Program!")
  print("This program is full of issues.")

# Improper variable names and missing quotes
name= input("Enter your name:)

# No error handling for invalid input
try:
  age = int(input("Enter your age: "))
except:
  pass

# No use of variables or meaningful operations
print("You are ", age, " years old!")

# Unnecessary use of global variable
global_var = 42

# Misleading comments
# This function does nothing useful
def misleading_function():
    # An empty block of code
    pass

# Poor naming and unnecessary calculations
def add_two_numbers(x, y):
    return x * 2 + y * 2

# Overuse of nested loops
for i in range(5):
    for j in range(5):
        for k in range(5):
            print(i, j, k)

# Incorrect use of list comprehension
numbers = [1, 2, 3, 4, 5]
squared_numbers = [i**2 for i in numbers if i % 2 == 0]

# Using print as a debugging tool
print("The squared numbers are:", squared_numbers)

# Unnecessary repetition of code
def repeat_function():
    print("This function is repeated.")
    print("This function is repeated.")
    print("This function is repeated.")

# Using mutable default argument
def bad_default_arg(items=[]):
    items.append(42)
    return items

# Calling the bad_default_arg multiple times
print(bad_default_arg())
print(bad_default_arg())
print(bad_default_arg())

# Ignoring exceptions
try:
    result = 10 / 0
except Exception as e:
    pass

# No function call

