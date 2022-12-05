from  ast import parse, Assign, Import, ImportFrom, dump, stmt
from typing import List
import argparse
import astunparse

output = "./output.py"

def get_pandas_alias(programbody : List[stmt]) -> str:
    for thing in programbody:
        if isinstance(thing, Import):
            name = thing.names[0].name
            alias = thing.names[0].asname
            if name == "pandas":
                return (alias if alias else name)
        
def get_connection(programbody : List[stmt], pd : str) -> str:
    return ""

def function_def_to_postgres_udf(programbody, conn, used_functions)-> List[str]:
    return 

def convert(filepath):
    program = None
    with open(filepath, "r") as file:
        program = parse(file.read())
    pd = get_pandas_alias(program.body)
    print(pd)

    for thing in program.body:
        print(type(thing))
        if isinstance(thing, Assign):
            print(thing)

    connectionname = get_connection(program.body, pd) 
    relevant_function_names : List[str] = []
    functions = function_def_to_postgres_udf(program.body, connectionname, relevant_function_names)

    # create_output()

    # final_read_sql = create_final_read_sql()

    # print(program.body)
    #  print(astunparse.unparse(program))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Converter")
    parser.add_argument("filepath", type=str, help="path to file that is to be converted")
    args = parser.parse_args()
    convert(args.filepath)
