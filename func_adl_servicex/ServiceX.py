# Code to support running an ast at a remote func-adl server.
from abc import ABC, abstractmethod
import ast
import logging
from typing import Any, Union, cast

import qastle
from qastle import python_ast_to_text_ast
from servicex import ServiceXDataset

from func_adl import EventDataset


class FuncADLServerException (Exception):
    'Thrown when an exception happens contacting the server'
    def __init__(self, msg):
        Exception.__init__(self, msg)


class ServiceXDatasetSourceBase (EventDataset, ABC):
    '''
    Base class for a ServiceX backend dataset.
    '''
    # How we map from func_adl to a servicex query
    _ds_map = {
        'ResultTTree': 'get_data_rootfiles_async',
        'ResultParquet': 'get_data_parquet_async',
        'ResultPandasDF': 'get_data_pandas_df_async',
        'ResultAwkwardArray': 'get_data_awkward_async',
    }

    def __init__(self, sx: ServiceXDataset):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__()

        self._ds = sx

    @abstractmethod
    def check_data_format_request(self, f_name: str):
        '''Check to make sure the dataformat that is getting requested is ok. Throw an error
        to give the user enough undersanding of why it isn't.

        Args:
            f_name (str): The function name of the final thing we are requesting.
        '''

    @abstractmethod
    def generate_qastle(self, a: ast.Call) -> str:
        '''Generate the qastle from the ast of the query.

        1. The top level function is already marked as being "ok"
        1. The top level function is included in this ast (you get the complete thing)

        Args:
            a (ast.AST): The complete AST of the request.

        Returns:
            str: Qastle that should be sent to servicex
        '''

    async def execute_result_async(self, a: ast.AST) -> Any:
        r'''
        Run a query against a func-adl ServiceX backend. The appropriate part of the AST is
        shipped there, and it is interpreted.

        Arguments:

            a:                  The ast that we should evaluate

        Returns:
            v                   Whatever the data that is requested (awkward arrays, etc.)
        '''
        # Now, make sure the ast is formed in a way we can deal with.
        if not isinstance(a, ast.Call):
            raise FuncADLServerException(f'Unable to use ServiceX to fetch a {a}.')
        a_func = a.func
        if not isinstance(a_func, ast.Name):
            raise FuncADLServerException(f'Unable to use ServiceX to fetch a call from {ast.dump(a_func)}')

        # Check the call is legal for this datasource.
        self.check_data_format_request(a_func.id)

        # Get the qastle string for this query
        q_str = self.generate_qastle(a)
        logging.getLogger(__name__).debug(f'Qastle string sent to servicex: {q_str}')

        # Next, run it, depending on the function
        if a_func.id not in self._ds_map:
            raise FuncADLServerException(f'Internal error - asked for {a_func.id} - but this dataset does not support it.')
        name = self._ds_map[a_func.id]
        attr = getattr(self._ds, name)

        # Run it!
        return await attr(q_str)


class ServiceXSourceXAOD(ServiceXDatasetSourceBase):
    def __init__(self, sx: Union[ServiceXDataset, str]):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        # Get the base created
        if isinstance(sx, str):
            ds = ServiceXDataset(sx, backend_type='xaod')
        else:
            ds = sx

        super().__init__(ds)

    def check_data_format_request(self, f_name: str):
        '''Check to make sure things we are asking for here are ok. We really can't deal with Parquet files. Other than
        that we are a go!

        Args:
            f_name (str): The function name we should check
        '''
        if f_name == 'ResultParquet':
            raise FuncADLServerException('The AsParquetFiles datatype is not supported by the xAOD backend. Please use AsROOTTTrees, AsAwkward, or AsPandas')

    def generate_qastle(self, a: ast.Call) -> str:
        '''Genrate the `qastle` for a query to the xAOD backend

        Args:
            a (ast.AST): The query

        Returns:
            str: The `qastle`, ready to pass to the back end.
        '''
        # If this is a call for an awkward or pandas, then we need to convert it to a root tree array.
        source = a
        if cast(ast.Name, a.func).id != 'ResultTTree':
            if len(a.args) != 2:
                raise FuncADLServerException(f'Do not understand how to call {cast(ast.Name, a.func).id} - wrong number of arguments')
            stream = a.args[0]
            cols = a.args[1]
            source = ast.Call(func=ast.Name(id='ResultTTree', ctx=ast.Load()), args=[stream, cols, ast.Str('treeme'), ast.Str('file.root')])

        return python_ast_to_text_ast(source)


class ServiceXSourceUpROOT(ServiceXDatasetSourceBase):
    def __init__(self, sx: Union[ServiceXDataset, str], treename: str):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        # Get the base created.
        if isinstance(sx, str):
            ds = ServiceXDataset(sx, backend_type='uproot')
        else:
            ds = sx

        super().__init__(ds)

        # Modify the argument list in EventDataSset to include the tree name.
        self.query_ast.args.append(ast.Str(s=treename))  # type: ignore

    def check_data_format_request(self, f_name: str):
        '''Check to make sure things we are asking for here are ok. We really can't deal with Parquet files. Other than
        that we are a go!

        Args:
            f_name (str): The function name we should check
        '''
        if f_name == 'ResultTTree':
            raise FuncADLServerException('The AsROOTTTrees datatype is not supported by the xAOD backend. Please use AsParquetFiles, AsAwkward, or AsPandas')

    def generate_qastle(self, a: ast.Call) -> str:
        '''Genrate the `qastle` for a query to the uproot backend

        Args:
            a (ast.AST): The query

        Returns:
            str: The `qastle`, ready to pass to the back end.
        '''
        # We need to pull the top off it - the request for the particular data type (parquet, pandas, etc.)
        # should not get passed to the transformer.
        source = a.args[0]

        # And that is all we need!
        return python_ast_to_text_ast(qastle.insert_linq_nodes(source))
