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




# Get datatype of first argument of FunctionDef
print(parse("""
def commment_len(comment: str) -> int:
    return str(len(comment))
""").body[0].args.args[0].annotation.id)

print(parse("""
def commment_len(comment: str) -> int:
    return str(len(comment))
""").body[0].args.args[0].arg)

# returntype of function
print(parse("""
def commment_len(comment: str) -> int:
    return str(len(comment))
""").body[0].returns.id)



