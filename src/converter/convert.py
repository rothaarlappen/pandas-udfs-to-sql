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
)
from typing import Any, List, Tuple
import argparse
import astunparse
import json
from collections.abc import Iterable
from os import path, remove

# TODO: Wrapper around that, which checks if type is actually supported
# Pyhton to Postgres
datatypeconversion = {
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


class UdfTransformer(NodeTransformer):
    def visit(self, node: AST) -> Any:
        if isinstance(node, Assign):
            return Assign()
        return node


# TODO: @Fabian what do you think of such Classes that contain information about parsing and "unparsing" of ast?
class ApplyOperator:
    def __init__(self, assign: Assign):
        self.new_column = assign.targets[0].slice.value.value
        self.passed_columns = (
            assign.value.args[0].body.args[0].slice.value.value
        )  # currently only one parameter... Should change in the future, lol
        self.invoked_function = assign.value.args[0].body.func.id
        self.dataFrame = assign.targets[0].value.id
        print(self.passed_columns, self.new_column)

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
    CREATE OR REPLACE FUNCTION {name} ({args.args[0].arg} {datatypeconversion[args.args[0].annotation.id]})
    RETURNS {datatypeconversion[function_ast.returns.id]}
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


def read_sql_ast(sql: str, apply: List[ApplyOperator]) -> Assign:
    return parse(sql)


def get_sql_for_applyOperators(applyOperators: ApplyOperator):
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


def convert_pipeline(filepath: str, persist_mode: str):
    outpaths = converted_file_path(filepath)

    setup_path = outpaths[0]
    pipeline_path = outpaths[1]

    for outpath in outpaths:
        if path.isfile(outpath):
            remove(outpath)

    program = parse_python_code(filepath)

    dataframes_in_db = set(["df"])
    applyoperators: List[ApplyOperator] = []

    # hardcoded because we can assume we get this one from switcheroo?
    # conn = "conn = create_connection()"
    dfloading: str = (
        "df = pd.read_sql('SELECT * FROM udfs.orders_tpch LIMIT 100', conn)"
    )

    udf_definitions: List[str] = []

    for thing in program.body:
        # TODO: sophisticated approach to detect read on df
        for dataframe in return_used_variables(dataframes_in_db, thing):
            # no read detected:
            if isinstance(thing, Assign):
                continue
            # read detected, execute queries/applies to load df
            else:
                projection = " *," + get_sql_for_applyOperators(
                    [
                        applyOperator
                        for applyOperator in applyoperators
                        if applyOperator.dataFrame == dataframe
                    ]
                )
                if persist_mode == "MATERIALIZED_VIEW":
                    append_ast_to_file(
                        parse(
                            f"""conn.execute("DROP MATERIALIZED VIEW IF EXISTS table_new")\n"""
                            + f"""conn.execute("CREATE MATERIALIZED VIEW table_new AS SELECT {projection} FROM orders", conn)"""
                        ),
                        pipeline_path,
                    )
                elif persist_mode == "NEW_TABLE":
                    append_ast_to_file(
                        parse(
                            f"""conn.execute("DROP TABLE IF EXISTS table_new")\n"""
                            + f"""conn.execute("CREATE TABLE table_new AS SELECT {projection} FROM orders", conn)"""
                        ),
                        pipeline_path,
                    )

        if isinstance(thing, Import) or isinstance(thing, ImportFrom):
            append_ast_to_files(thing, outpaths)
        if isinstance(thing, Expr):
            if (
                astunparse.unparse(thing).strip().startswith("sys")
            ):  # We should probably check if expression needs df...... @Fabian? ^^
                append_ast_to_files(thing, outpaths)
            elif contains_to_sql(thing):
                pass
            else:
                append_ast_to_file(thing, pipeline_path)

        if isinstance(thing, FunctionDef):
            udf = to_ast_that_creates_udf(thing, "conn")
            append_ast_to_file(udf, setup_path)
            # udf_definitions.append(astunparse.unparse(udf))

        if isinstance(thing, Assign):
            if contains_apply(thing):
                applyoperators.append(ApplyOperator(thing))
            elif contains_read_sql(thing):
                dataframes_in_db.add(thing.targets[0].id)
            else:
                append_ast_to_files(thing, outpaths)

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
