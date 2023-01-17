from os import path
from typing import Tuple
from typing import List
from ast import parse, Assign, Expr, AST
from collections.abc import Iterable
import astunparse


# TODO: Wrapper around that, which checks if type is actually supported
# Python to Postgres
DATATYPE_MAPPING = {
    "str": "text",
    "int": "integer",
    "bool": "bool",
    "Timestamp": "date",
}


def converted_files_paths(originial_pipeline: str) -> Tuple[str, str]:
    setup_file = "converted_setup_" + path.basename(originial_pipeline)
    pipeline_file = "converted_" + path.basename(originial_pipeline)
    dir = path.dirname(path.abspath(originial_pipeline))
    setup_file_path = path.join(dir, setup_file)
    pipeline_file_path = path.join(dir, pipeline_file)
    return (setup_file_path, pipeline_file_path)


def parse_python_code(filepath):
    program = None
    with open(filepath, "r") as file:
        program = parse(file.read())
    assert program != None, "program should be parsed"
    return program


def append_ast_to_file(program_ast, filepath):
    with open(filepath, "a") as file:
        file.write(astunparse.unparse(program_ast))


# weak limitation... should check for df.apply or something else...
def contains_apply(assign: Assign) -> bool:
    try:
        return assign.value.func.attr == "apply"
    except:
        # Antipattern it is...
        return False


def contains_read_sql(assign: Assign) -> bool:
    try:
        return assign.value.func.attr == "read_sql"
    except:
        # Antipattern it is...
        return False


def contains_to_sql(expr: Expr):
    try:
        return expr.value.func.attr == "to_sql"
    except:
        return False


def contains_head(expr: Expr):
    try:
        return expr.value.func.attr == "head"
    except:
        return False


def return_used_variables(variables: List[str], ast_node: AST) -> List[str]:
    hit_variables = set()
    for field in ast_node._fields:
        attr = getattr(ast_node, field)
        if isinstance(attr, Iterable) and not isinstance(attr, str):
            for sub_attr in attr:
                if isinstance(sub_attr, AST):
                    hit_variables.update(return_used_variables(variables, sub_attr))
                elif sub_attr in variables:
                    hit_variables.add(sub_attr)
        elif isinstance(attr, AST):
            hit_variables.update(return_used_variables(variables, attr))
        elif attr in variables:
            hit_variables.add(attr)
    return hit_variables
