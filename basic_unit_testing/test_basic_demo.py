from codecs import namereplace_errors


def greet(name):
    return f'hello {name}'

def test_greet():
    # STEP 1: Arrange (Arrange the input data for the method. Also have the expected_output ready)
    name = "Usman"                      # Input

    expected_ouput = "Hello Usman"      # Expected Output

    # STEP 2: Act (Call the Code/Function under test)
    actual_output = greet(name)

    # STEP 3: Assert (compare expected and actual outputs)
    assert  actual_output == expected_ouput
