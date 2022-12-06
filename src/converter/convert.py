from ast import parse, Assign, Import, stmt, NodeTransformer, AST, FunctionDef, dump, Expr
from typing import Any, List,Dict
import argparse
import astunparse
import json

# TODO: Wrapper around that, which checks if type is actually supported
# Pyhton to Postgres
datatypeconversion = {
    "str" : "text",
    "int" : "integer",
    "bool" : "bool"
}

class UdfTransformer(NodeTransformer):
    def visit(self, node: AST) -> Any:
        if isinstance(node, Assign):
            return Assign()
        return node

# TODO: @Fabian what do you think of such Classes that contain information about parsing and "unparsing" of ast? 
class ApplyOperator():
    def __init__(self, assign: Assign):
        self.new_column = assign.targets[0].slice.value
        self.passed_columns = assign.value.args[0].body.args[0].slice.value # currently only one parameter... Should change in the future, lol
        self.invoked_function = assign.value.args[0].body.func.id
    def __str__(self):
        return json.dumps({"newcol":self.new_column, "passedcol": self.passed_columns, "invoked_function": self.invoked_function})
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

def append_ast_to_file(program_ast, filepath ="./out"):
    with open(filepath, "a") as file:
        file.write(astunparse.unparse(program_ast))

# TODO: conn is hardcoded (should use the "global-connection" in program...)
# TODO: should parse all arguments of FunctionDef-argumentlist
def to_ast_that_craetes_udf(function_ast : FunctionDef, conn : str) -> Expr:
    name = function_ast.name
    args = function_ast.args

    udf_creating_ast = parse(f"""conn.execute(\"\"\" 
    CREATE OR REPLACE FUNCTION {name} ({args.args[0].arg} {datatypeconversion[args.args[0].annotation.id]})
    RETURNS {datatypeconversion[function_ast.returns.id]}
    AS $$
        {astunparse.unparse(function_ast.body)}
    $$ LANGUAGE plpython3u;
    \"\"\")    
    """)

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

def read_sql_ast(sql: str, apply : List[ApplyOperator]) -> Assign:
    return parse(sql)

def convert(filepath, outpath):
    program = parse_python_code(filepath)
    # pd = get_pandas_alias(program.body)
    # print(pd)

    applyoperators : List[ApplyOperator] = []
    
    # hardcoded because we can assume we get this one from switcheroo?
    conn = "conn = create_connection()"
    dfloading : str = "df = pd.load_sql('SELECT * FROM udfs.orders_tpch', conn)"
    
    udf_definitions : List[str] = []

    for thing in program.body:
        if isinstance(thing, Import):
            append_ast_to_file(thing)
        if isinstance(thing, Expr):
            append_ast_to_file(thing)  
        if isinstance(thing, FunctionDef):
            udf = to_ast_that_craetes_udf(thing, "conn") 
            append_ast_to_file(udf)
            # udf_definitions.append(astunparse.unparse(udf))

        if isinstance(thing, Assign):
            if (contains_apply(thing)):
                applyoperators.append(ApplyOperator(thing))
            elif (contains_read_sql(thing)):
                continue
            else:
                append_ast_to_file(thing)
            # print(astunparse.unparse(returns))
            # print(thing.args.args[0].annotation.id)
            # print(thing.returns)
    append_ast_to_file(read_sql_ast(dfloading, applyoperators))


    # for apply in applyoperators:
    #     print(apply.to_sql_projection())

    # for udf in udf_definitions:
    #     print(udf)


    # print output....
    # 1. print all imports
    # 2. print connection-generation (maybe hardcode this for now as this would be sth we would get from the switcheroo-pipeline...)
    # 3. print udf_definitions to create udfs
    # 4. generate the final df.load_sql() invoking the udfs we defined beforehand.... (maybe hardcode the initial df.load as well, cause we would get this one as well and can assume it contains SELECT * FROM table)
    # -> df = df.load_sql("SELECT *, applyoperator[0].to_sql_projection(), applyoperator[1].to_sql_projection FROM table", conn)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Converter")
    parser.add_argument(
        "filepath", type=str, help="path to file that is to be converted"
    )
    args = parser.parse_args()
    convert(args.filepath, outpath="./output.py")
