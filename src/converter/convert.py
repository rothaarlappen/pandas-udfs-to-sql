import ast
import argparse
import astunparse
# TODO: parse args
output = "./output.py"


def convert(filepath):
    program = None
    with open(filepath, "r") as file:
        program = ast.parse(file.read())
    print(program.body)
    for thing in program.body:
        print(type(thing))

    print(astunparse.unparse(program))
    print("How do we benchmark this? ")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Converter")
    parser.add_argument("filepath", type=str, help="path to file that is to be converted")
    args = parser.parse_args()
    convert(args.filepath)
