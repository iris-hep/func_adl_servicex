# Code to support running an ast at a remote func-adl server.
import ast
import logging
from typing import Any, Optional, Union

import qastle
from qastle import python_ast_to_text_ast
from servicex import ServiceXDataset

from func_adl import EventDataset


class FuncADLServerException (Exception):
    'Thrown when an exception happens contacting the server'
    def __init__(self, msg):
        Exception.__init__(self, msg)


class ServiceXDatasetSource (EventDataset):
    '''
    A dataset for a func_adl query that is located on a ServiceX backend.
    '''
    def __init__(self, sx: Union[ServiceXDataset, str], treename: Optional[str] = None):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__()

        if isinstance(sx, str):
            self._ds = ServiceXDataset(sx)
        else:
            self._ds = sx

        # If we are doing a tree, modify the argument list to include a tree.
        if treename is not None:
            self.query_ast.args.append(ast.Str(s=treename))

        # TODO: #1 Code is custom tailored to deal with uproot vs xAOD here. Differences should be removed.
        self._is_uproot = treename is not None

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

        if self._is_uproot:
            # The uproot transformer only returns parquet files at the moment. So we had better look something like that, or something
            # we can convert from.

            if a_func.id == 'ResultParquet':
                # For now, we have to strip off the ResultParquet and send the rest down to uproot.
                source = a.args[0]
                q_str = python_ast_to_text_ast(qastle.insert_linq_nodes(source))
                logging.debug(f'Qastle string sent to uproot query: {q_str}')
                return await self._ds.get_data_parquet_async(q_str)
            elif a_func.id == 'ResultPandasDF':
                raise NotImplementedError()
            elif a_func.id == 'ResultAwkwardArray':
                raise NotImplementedError()
            else:
                raise FuncADLServerException(f'Unable to use ServiceX to fetch a result in the form {a_func.id} - Only ResultParquet, ResultPandasDF and ResultAwkwardArray are supported')

        else:
            # If we are xAOD then we can come back with a pandas df, awkward array, or root files.
            # TODO: #2 Add root files as a legal return type here.
            if a_func.id == 'ResultPandasDF':
                source = a.args[0]
                cols = a.args[1]
                top_level_ast = ast.Call(func=ast.Name('ResultTTree'), args=[source, cols, ast.Str('treeme'), ast.Str('file.root')])
                q_str = python_ast_to_text_ast(top_level_ast)
                logging.debug(f'Qastle string sent to xAOD query: {q_str}')
                return await self._ds.get_data_pandas_df_async(q_str)
            elif a_func.id == 'ResultAwkwardArray':
                source = a.args[0]
                cols = a.args[1]
                top_level_ast = ast.Call(func=ast.Name('ResultTTree'), args=[source, cols, ast.Str('treeme'), ast.Str('file.root')])
                q_str = python_ast_to_text_ast(top_level_ast)
                logging.debug(f'Qastle string sent to xAOD query: {q_str}')
                return await self._ds.get_data_awkward_async(q_str)
            elif a_func.id == 'ResultTTree':
                raise NotImplementedError()
            else:
                raise FuncADLServerException(f'Unable to use ServiceX to fetch a result in the form {a_func.id} - Only ResultTTree, ResultPandasDF and ResultAwkwardArray are supported')
