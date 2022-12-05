from ast import parse, Assign, Import, stmt, NodeTransformer, AST
from typing import Any, List
import argparse
import astunparse


class UdfTransformer(NodeTransformer):
    def visit(self, node: AST) -> Any:
        if isinstance(node, Assign):
            return Assign()
        return node


def get_pandas_alias(programbody: List[stmt]) -> str:
    for thing in programbody:
        if isinstance(thing, Import):
            name = thing.names[0].name
            alias = thing.names[0].asname
            if name == "pandas":
                return alias if alias else name


def get_connection(programbody: List[stmt], pd: str) -> str:
    return ""


def function_def_to_postgres_udf(programbody, conn, used_functions) -> List[str]:
    return


def parse_python_code(filepath):
    program = None
    with open(filepath, "r") as file:
        program = parse(file.read())
    assert program != None, "program should be parsed"
    return program


def ast_to_file(program_ast, filepath):
    with open(filepath, "w") as file:
        file.write(astunparse.unparse(program_ast))


def convert(filepath, outpath):
    program = parse_python_code(filepath)
    pd = get_pandas_alias(program.body)
    print(pd)

    for thing in program.body:
        print(type(thing))
        if isinstance(thing, Assign):
            print(thing)

    connection_name = get_connection(program.body, pd)
    relevant_function_names: List[str] = []
    functions = function_def_to_postgres_udf(
        program.body, connection_name, relevant_function_names
    )

    ast_to_file(program, outpath)
    # create_output()

    # final_read_sql = create_final_read_sql()

    # print(program.body)
    #  print(astunparse.unparse(program))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Converter")
    parser.add_argument(
        "filepath", type=str, help="path to file that is to be converted"
    )
    args = parser.parse_args()
    convert(args.filepath, outpath="./output.py")
