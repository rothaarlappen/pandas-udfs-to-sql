from ast import (
    parse,
    Assign,
    Import,
    NodeTransformer,
    AST,
    FunctionDef,
    Expr,
    ImportFrom,
    NodeVisitor,
    Name,
)
from typing import Any, List
import argparse
import astunparse
import json
from os import path, remove

from .utils import (
    converted_files_paths,
    parse_python_code,
    append_ast_to_file,
    contains_apply,
    contains_to_sql,
    contains_read_sql,
    contains_head,
)


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


def get_sql_for_applyOperators(applyOperators: List[ApplyOperator]):
    return ",".join(
        [applyOperator.to_sql_projection() for applyOperator in applyOperators]
    )


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
        self.functions[node.name] = {
            "node": node,
            "body": node.body,
            "arguments": node.args.args,
        }

    def visit_Assign(self, node: Assign) -> None:
        if isinstance(node.targets[0], Name):
            self.last_assigns[node.targets[0].id] = node
        if contains_apply(node):
            self.apply_operators.append(ApplyOperator(node))
        elif contains_read_sql(node):
            self.connection_variables.add(node.value.args[1].id)
            self.dataframes.add(node.targets[0].id)


class PipelineConverter(NodeTransformer):
    def __init__(self, udf_visitor: UDFVisitor, persist_mode: str) -> None:
        self.udf_visitor = udf_visitor
        self.persist_mode = persist_mode

    def visit(self, node: AST) -> Any:
        # TODO: actually get correct dataframe
        projection = " *," + get_sql_for_applyOperators(
            self.udf_visitor.apply_operators
        )
        if contains_to_sql(node):
            assert self.persist_mode in ["MATERIALIZED_VIEW", "NEW_TABLE"]
            return parse(
                f"""conn.execute("DROP {"TABLE" if self.persist_mode == "NEW_TABLE" else "MATERIALIZED VIEW"} IF EXISTS table_new")\n"""
                + f"""conn.execute("CREATE {"TABLE" if self.persist_mode == "NEW_TABLE" else "MATERIALIZED VIEW"} table_new AS SELECT {projection} FROM orders", conn)"""
            )

        elif contains_head(node):
            return parse(f"df = pd.read_sql('SELECT {projection} FROM orders', conn)")
        return super().visit(node)

    def visit_Assign(self, node: Assign) -> Any:
        if contains_apply(node) or contains_read_sql(node):
            return None
        return node


class SetupGenerator:
    def __init__(self, udf_visitor: UDFVisitor) -> None:
        self.udf_visitor = udf_visitor

    def create_udf_with_inferred_types(self, function_name: str):
        # TODO: get correct connection variable out of list
        # TODO: support multiple args
        applies = [
            apply
            for apply in self.udf_visitor.apply_operators
            if apply.invoked_function == function_name
        ]
        assert len(applies) > 0
        apply: ApplyOperator = applies[0]

        function_info = self.udf_visitor.functions[function_name]
        return parse(
            f"""{function_name}_example_input = {list(self.udf_visitor.connection_variables)[0]}.execute("SELECT {apply.passed_columns} FROM orders LIMIT 1").fetchall()[0][0]\n"""
            + f"""{function_name}_input_type = type({function_name}_example_input)\n"""
            + f"""{function_name}_output_type = type({function_name}({function_name}_example_input))\n"""
            + f"""{list(self.udf_visitor.connection_variables)[0]}.execute(f\"\"\"
                CREATE OR REPLACE FUNCTION {function_name} ({function_info["arguments"][0].arg} {{DATATYPE_MAPPING[{function_name}_input_type]}})
                RETURNS {{DATATYPE_MAPPING[{function_name}_output_type]}}
                AS $${astunparse.unparse(function_info["body"])}$$
                LANGUAGE plpython3u
                PARALLEL SAFE;
            \"\"\")"""
        )

    def get_datatype_mapping(self):
        return parse(
            """DATATYPE_MAPPING = {
    str: "text",
    int: "integer",
    bool: "bool",
    Timestamp: "date",
}"""
        )

    def generate_setup(self):
        setup = []
        setup += self.udf_visitor.imports
        setup.append(self.get_datatype_mapping())
        setup += [function["node"] for function in self.udf_visitor.functions.values()]
        setup += [
            self.udf_visitor.last_assigns[connection]
            for connection in self.udf_visitor.connection_variables
        ]
        setup += [
            self.create_udf_with_inferred_types(function)
            for function in self.udf_visitor.functions.keys()
        ]
        return setup


def convert_pipeline(filepath: str, persist_mode: str):
    outpaths = converted_files_paths(filepath)

    setup_path = outpaths[0]
    pipeline_path = outpaths[1]

    for outpath in outpaths:
        if path.isfile(outpath):
            remove(outpath)

    tree = parse_python_code(filepath)
    udf_visitor = UDFVisitor()
    udf_visitor.visit(tree)
    pipeline_converter = PipelineConverter(udf_visitor, persist_mode)

    setup_generator = SetupGenerator(udf_visitor)
    setup = setup_generator.generate_setup()

    for node in setup:
        append_ast_to_file(node, setup_path)

    converted_pipeline = pipeline_converter.visit(tree)
    for node in converted_pipeline.body:
        append_ast_to_file(node, pipeline_path)

    return outpaths


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
