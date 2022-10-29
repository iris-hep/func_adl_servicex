import ast
from typing import Optional, cast


def has_col_names(a: ast.AST) -> bool:
    """Determine if any column names were specified
    in this request.

    Args:
        a (ast.AST): The complete AST of the request.

    Returns:
        bool: True if no column names were specified, False otherwise.
    """
    assert isinstance(a, ast.Call)
    func_ast = a
    top_function = cast(ast.Name, a.func).id

    if top_function == "ResultAwkwardArray":
        if len(a.args) >= 2:
            cols = a.args[1]
            if isinstance(cols, ast.List):
                if len(cols.elts) > 0:
                    return True
            elif isinstance(ast.literal_eval(cols), str):
                return True
        func_ast = a.args[0]
        assert isinstance(func_ast, ast.Call)

    top_function = cast(ast.Name, func_ast.func).id
    if top_function not in ["Select", "SelectMany"]:
        return False

    # Grab the lambda and see if it is returning a dict
    func_called = func_ast.args[1]
    assert isinstance(func_called, ast.Lambda)
    body = func_called.body
    if isinstance(body, ast.Dict):
        return True

    # Ok - we didn't find evidence of column names being
    # specified. It could still happen, but not as far
    # as we can tell.

    return False


def has_tuple(a: ast.AST) -> bool:
    """Determine if this query used tuples in its final result

    NOTE: This can't do depth searches in a really complex
    query - then you need to follow best practices.

    Args:
        a (ast.AST): The query

    Returns:
        bool: True if the final Select statement has tuples or not.
    """

    def find_Select(a: ast.AST) -> Optional[ast.Call]:
        # Descent the call chain until we find a Select.
        while isinstance(a, ast.Call):
            if isinstance(a.func, ast.Name):
                if cast(ast.Name, a.func).id == "Select":
                    return a
            a = a.args[0]
        return None

    select_statement = find_Select(a)
    if select_statement is None:
        return False

    # Ok - we have a select statement. Now we need to see if it is returning a tuple.
    func_called = select_statement.args[1]
    assert isinstance(func_called, ast.Lambda)
    body = func_called.body
    return isinstance(body, ast.Tuple)
