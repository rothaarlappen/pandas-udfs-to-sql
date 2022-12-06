from ast import parse, Assign, Import, stmt, NodeTransformer, AST, FunctionDef, dump, Expr, Attribute, Lambda
from typing import Any, List
import argparse
import astunparse


# get name of new column
print( parse("""
df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
""").body[0].targets[0].slice.value)


# called function -> apply()
print(parse("""
df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
""").body[0].value.func.attr)

# called function (== name of udf)
print(parse("""
df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
""").body[0].value.args[0].body.func.id)

# check if lambda is used in apply
print(isinstance( parse("""
df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
""").body[0].value.args[0], Lambda))

# used column
print(parse("""
df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
""").body[0].value.args[0].body.args[0].slice.value)