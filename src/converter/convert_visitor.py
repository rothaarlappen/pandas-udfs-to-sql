from ast import (
    parse,
    Assign,
    Import,
    stmt,
    NodeTransformer,
    AST,
    FunctionDef,
    Expr,
    ImportFrom,
    NodeVisitor,
    Name
)
from typing import Any, List, Tuple
import argparse
import astunparse
import json
from collections.abc import Iterable
from os import path, remove

# TODO: Wrapper around that, which checks if type is actually supported
# Pyhton to Postgres
DATATYPE_MAPPING = {
    "str": "text",
    "int": "integer",
    "bool": "bool",
    "Timestamp": "date",
}


def converted_file_path(originial_pipeline: str) -> Tuple[str, str]:
    setup_file = "converted_setup_" + path.basename(originial_pipeline)
    pipeline_file = "converted_" + path.basename(originial_pipeline)
    dir = path.dirname(path.abspath(originial_pipeline))
    setup_file_path = path.join(dir, setup_file)
    pipeline_file_path = path.join(dir, pipeline_file)
    return (setup_file_path, pipeline_file_path)


# TODO: @Fabian what do you think of such Classes that contain information about parsing and "unparsing" of ast?
class ApplyOperator:
    def __init__(self, assign: Assign):
        self.new_column = assign.targets[0].slice.value
        self.passed_columns = (
            assign.value.args[0].body.args[0].slice.value
        )  # currently only one parameter... Should change in the future, lol
        self.invoked_function = assign.value.args[0].body.func.id
        self.dataFrame = assign.targets[0].value.id

    def __str__(self):
        return json.dumps(
            {
                "newcol": self.new_column,
                "passedcol": self.passed_columns,
                "invoked_function": self.invoked_function,
            }
        )

    # this method probably needs a renaming
    def to_sql_projection(self):
        return f"{self.invoked_function}({self.passed_columns}) AS {self.new_column}"


def get_pandas_alias(programbody: List[stmt]) -> str:
    for thing in programbody:
        if isinstance(thing, Import):
            name = thing.names[0].name
            alias = thing.names[0].asname
            if name == "pandas":
                return alias if alias else name


def parse_python_code(filepath):
    program = None
    with open(filepath, "r") as file:
        program = parse(file.read())
    assert program != None, "program should be parsed"
    return program


def append_ast_to_files(program_ast, filepaths: List[str]):
    for file in filepaths:
        append_ast_to_file(program_ast, file)


def append_ast_to_file(program_ast, filepath):
    with open(filepath, "a") as file:
        file.write(astunparse.unparse(program_ast))


# TODO: conn is hardcoded (should use the "global-connection" in program...)
# TODO: should parse all arguments of FunctionDef-argumentlist
def to_ast_that_creates_udf(function_ast: FunctionDef, conn: str) -> Expr:
    name = function_ast.name
    args = function_ast.args

    udf_creating_ast = parse(
        f"""conn.execute(\"\"\"
    CREATE OR REPLACE FUNCTION {name} ({args.args[0].arg} {DATATYPE_MAPPING[args.args[0].annotation.id]})
    RETURNS {DATATYPE_MAPPING[function_ast.returns.id]}
    AS $$
        {astunparse.unparse(function_ast.body)}
    $$ LANGUAGE plpython3u
    PARALLEL SAFE;
    \"\"\")
    """
    )

    return udf_creating_ast


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


def read_sql_ast(sql: str, apply: List[ApplyOperator]) -> Assign:
    return parse(sql)


def get_sql_for_applyOperators(applyOperators: List[ApplyOperator]):
    return ",".join(
        [applyOperator.to_sql_projection() for applyOperator in applyOperators]
    )


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

class UDFVisitor(NodeVisitor):

    def __init__(self) -> None:
        self.imports = []
        self.dataframes = set()
        self.functions = {}
        self.apply_operators = []
        self.connection_variables = set()
        self.last_assigns = {}

    def visit_Import(self, node: Import) -> None:
        self.imports.append(node)

    def visit_ImportFrom(self, node: ImportFrom) -> None:
        self.imports.append(node)

    def visit_Expr(self, node: Expr) -> None:
        if (
                astunparse.unparse(node).strip().startswith("sys")
            ):  # We should probably check if expression needs df...... @Fabian? ^^
            self.imports.append(node)

    def visit_FunctionDef(self, node: FunctionDef) -> None:
        self.functions[node.name] = {"node" : node,
                                     "body": node.body,
                                     "arguments": node.args.args}

    def visit_Assign(self, node: Assign) -> None:
        if isinstance(node.targets[0], Name):
            self.last_assigns[node.targets[0].id] = node
        if contains_apply(node):
            self.apply_operators.append(ApplyOperator(node))
        elif contains_read_sql(node):
            self.connection_variables.add(node.value.args[1].id)
            self.dataframes.add(node.targets[0].id)

class PipelineConverter(NodeTransformer):

    def __init__(self, udf_visitor : UDFVisitor) -> None:
        self.udf_visitor = udf_visitor

    def visit(self, node: AST) -> Any:
        for dataframe in return_used_variables(self.dataframes, node):
            # no read detected:
            if isinstance(node, Assign):
                continue
            # handle applies here basically
            pass
        super().visit(node)

class SetupGenerator:

    def __init__(self, udf_visitor: UDFVisitor) -> None:
        self.udf_visitor = udf_visitor

    def create_udf_with_inferred_types(self, function_name : str):
        # TODO: get correct connection variable out of list
        applies = [apply for apply in self.udf_visitor.apply_operators if apply.invoked_function == function_name]
        assert(len(applies) > 0)
        apply : ApplyOperator = applies[0]

        function_info = self.udf_visitor.functions[function_name]
        return parse(
            f"""{function_name}_example_input = {list(self.udf_visitor.connection_variables)[0]}.execute("SELECT {apply.passed_columns} FROM orders LIMIT 1").fetchall()[0][0]
{function_name}_input_type = type({function_name}_example_input)
{function_name}_output_type = type({function_name}({function_name}_example_input))
{list(self.udf_visitor.connection_variables)[0]}.execute(f\"\"\"
    CREATE OR REPLACE FUNCTION {function_name} ({apply.passed_columns} {{DATATYPE_MAPPING[{function_name}_input_type]}})
    RETURNS {{DATATYPE_MAPPING[{function_name}_output_type]}}
    AS $$
        {astunparse.unparse(function_info["body"])}
    $$ LANGUAGE plpython3u
    PARALLEL SAFE;
    \"\"\")
    """)

    def get_datatype_mapping(self):
        return parse("""DATATYPE_MAPPING = {
    str: "text",
    int: "integer",
    bool: "bool",
    Timestamp: "date",
}""")
    def generate_setup(self):
        setup = []
        setup.append(self.get_datatype_mapping())
        setup += self.udf_visitor.imports
        setup += [function["node"] for function in self.udf_visitor.functions.values()]
        setup += [self.udf_visitor.last_assigns[connection] for connection in self.udf_visitor.connection_variables]
        setup += [self.create_udf_with_inferred_types(function) for function in self.udf_visitor.functions.keys()]
        return setup



def convert_pipeline(filepath: str, persist_mode: str):
    outpaths = converted_file_path(filepath)

    setup_path = outpaths[0]
    pipeline_path = outpaths[1]

    for outpath in outpaths:
        if path.isfile(outpath):
            remove(outpath)

    tree = parse_python_code(filepath)
    udf_visitor = UDFVisitor()
    udf_visitor.visit(tree)
    pipeline_converter = PipelineConverter(udf_visitor)

    setup_generator = SetupGenerator(udf_visitor)
    setup = setup_generator.generate_setup()

    for node in setup:
        append_ast_to_file(node, setup_path)
    #pipeline_converter.visit()



    return outpaths

    # print output....
    # 1. print all imports
    # 2. print connection-generation (maybe hardcode this for now as this would be sth we would get from the switcheroo-pipeline...)
    # 3. print udf_definitions to create udfs
    # 4. generate the final df.read_sql() invoking the udfs we defined beforehand.... (maybe hardcode the initial df.load as well, cause we would get this one as well and can assume it contains SELECT * FROM table)
    # -> df = df.read_sql("SELECT *, applyoperator[0].to_sql_projection(), applyoperator[1].to_sql_projection FROM table", conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Converter")
    parser.add_argument(
        "filepath", type=str, help="path to file that is to be converted"
    )
    parser.add_argument(
        "persist_mode",
        type=str,
        help="Store results as 'MATERIALIZED_VIEW' or 'NEW_TABLE'",
        default="MATERIALIZED_VIEW",
    )

    args = parser.parse_args()
    convert_pipeline(args.filepath, args.persist_mode)
