# Code to support running an ast at a remote func-adl server.
import ast
from typing import Any, Union

from func_adl import EventDataset
from qastle import python_ast_to_text_ast
from servicex import ServiceXDataset


class FuncADLServerException (Exception):
    'Thrown when an exception happens contacting the server'
    def __init__(self, msg):
        Exception.__init__(self, msg)


class ServiceXDatasetSource (EventDataset):
    '''
    A dataset for a func_adl query that is located on a ServiceX backend.
    '''
    def __init__(self, sx: Union[ServiceXDataset, str]):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        EventDataset.__init__(self)

        if isinstance(sx, str):
            self._ds = ServiceXDataset(sx)
        else:
            self._ds = sx

    async def execute_result_async(self, a: ast.AST) -> Any:
        r'''
        Run a query against a func-adl ServiceX backend. The appropriate part of the AST is
        shipped there, and it is interpreted.

        Arguments:

            a:                  The ast that we should evaluate

        Returns:
            v                   Whatever the data that is requested (awkward arrays, etc.)
        '''
        # Now, make sure the ast is formed in a way we cna deal with.
        if not isinstance(a, ast.Call):
            raise FuncADLServerException(f'Unable to use ServiceX to fetch a {a}.')
        a_func = a.func
        if not isinstance(a_func, ast.Name):
            raise FuncADLServerException(f'Unable to use ServiceX to fetch a call from {a_func}')

        # Make the servicex call, asking for the appropriate return type. Depending on the return-type
        # alter it so it can return something that ServiceX can understand.
        if a_func.id == 'ResultPandasDF':
            source = a.args[0]
            cols = a.args[1]
            top_level_ast = ast.Call(func=ast.Name('ResultTTree'), args=[source, cols, ast.Str('treeme'), ast.Str('file.root')])
            q_str = python_ast_to_text_ast(top_level_ast)
            return await self._ds.get_data_pandas_df_async(q_str)
        elif a_func.id == 'ResultAwkwardArray':
            source = a.args[0]
            cols = a.args[1]
            top_level_ast = ast.Call(func=ast.Name('ResultTTree'), args=[source, cols, ast.Str('treeme'), ast.Str('file.root')])
            q_str = python_ast_to_text_ast(top_level_ast)
            return await self._ds.get_data_awkward_async(q_str)
        else:
            raise FuncADLServerException(f'Unable to use ServiceX to fetch a result in the form {a_func.id} - Only ResultPandasDF and ResultAwkwardArray are supported')
