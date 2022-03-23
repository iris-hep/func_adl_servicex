import ast
from typing import cast


def has_col_names(a: ast.AST) -> bool:
    '''Determine if any column names were specified
    in this request.

    Args:
        a (ast.AST): The complete AST of the request.

    Returns:
        bool: True if no column names were specified, False otherwise.
    '''
    assert isinstance(a, ast.Call)
    func_ast = a
    top_function = cast(ast.Name, a.func).id

    if top_function == 'ResultAwkwardArray':
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
    if top_function not in ['Select', 'SelectMany']:
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
