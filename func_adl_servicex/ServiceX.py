# Code to support running an ast at a remote func-adl server.
import ast
import logging
from abc import ABC
from collections import Iterable
from typing import Any, Optional, Union, cast

from func_adl import EventDataset
from qastle import python_ast_to_text_ast
from servicex import ServiceXDataset
from servicex.utils import DatasetType


class FuncADLServerException (Exception):
    'Thrown when an exception happens contacting the server'
    def __init__(self, msg):
        Exception.__init__(self, msg)


class ServiceXDatasetSourceBase (EventDataset, ABC):
    '''
    Base class for a ServiceX backend dataset.

    While no methods are abstract, base classes will need to add arguments
    to the base `EventDataset` to make sure that it contains the information
    backends expect!
    '''
    # How we map from func_adl to a servicex query
    _ds_map = {
        'ResultTTree': 'get_data_rootfiles_async',
        'ResultParquet': 'get_data_parquet_async',
        'ResultPandasDF': 'get_data_pandas_df_async',
        'ResultAwkwardArray': 'get_data_awkward_async',
    }

    # If it comes down to format, what are we going to grab?
    _format_map = {
        'root': 'get_data_rootfiles_async',
        'parquet': 'get_data_parquet_async',
    }

    # These are methods that are translated locally
    _execute_locally = ['ResultPandasDF', 'ResultAwkwardArray']

    # If we have a choice of formats, what can we do, in
    # prioritized order?
    _format_list = ['parquet', 'root']

    def __init__(self, sx: Union[ServiceXDataset, DatasetType], backend_name: str):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__()

        # Get the base created
        if isinstance(sx, (str, Iterable)):
            ds = ServiceXDataset(sx, backend_name=backend_name)
        else:
            ds = sx
        self._ds = ds

        self._return_qastle = False

    @property
    def return_qastle(self) -> bool:
        '''Get/Set flag indicating if we'll generate `qastle` rather than run the query when executed.

        If `True`, then execution of this query will return `qastle`, and if `False` then
        the query will be executed.
        '''
        return self._return_qastle

    @return_qastle.setter
    def return_qastle(self, value: bool):
        self._return_qastle = value

    def check_data_format_request(self, f_name: str):
        '''Check to make sure the dataformat that is getting requested is ok. Throw an error
        to give the user enough undersanding of why it isn't.

        Args:
            f_name (str): The function name of the final thing we are requesting.
        '''
        if ((f_name == 'ResultTTree' and self._ds.first_supported_datatype('root') is None)
                or (f_name == 'ResultParquet'
                    and self._ds.first_supported_datatype('parquet') is None)):
            raise FuncADLServerException(f'{f_name} is not supported by {str(self._ds)}')

        # If here, we assume the user knows what they are doing. The return format will be
        # the default file type
        return

    def generate_qastle(self, a: ast.Call) -> str:
        '''Generate the qastle from the ast of the query.

        1. The top level function is already marked as being "ok"
        1. If the top level function is something we have to process locally,
           then we strip it off.

        Args:
            a (ast.AST): The complete AST of the request.

        Returns:
            str: Qastle that should be sent to servicex
        '''
        top_function = cast(ast.Name, a.func).id
        source = a
        if top_function in self._execute_locally:
            # Request the default type here
            default_format = self._ds.first_supported_datatype(['parquet', 'root'])
            assert default_format is not None, 'Unsupported ServiceX returned format'
            method_to_call = self._format_map[default_format]

            stream = a.args[0]
            col_names = a.args[1]
            if method_to_call == 'get_data_rootfiles_async':
                source = ast.Call(
                    func=ast.Name(id='ResultTTree', ctx=ast.Load()),
                    args=[stream, col_names, ast.Str('treeme'), ast.Str('junk.root')])
            elif method_to_call == 'get_data_parquet_async':
                source = ast.Call(
                    func=ast.Name(id='ResultParquet', ctx=ast.Load()),
                    args=[stream, col_names, ast.Str('junk.parquet')])
            else:  # pragma: no cover
                # This indicates a programming error
                assert False, f'Do not know how to call {method_to_call}'

        return python_ast_to_text_ast(source)

    async def execute_result_async(self, a: ast.Call, title: Optional[str] = None) -> Any:
        r'''
        Run a query against a func-adl ServiceX backend. The appropriate part of the AST is
        shipped there, and it is interpreted.

        Arguments:

            a:                  The ast that we should evaluate
            title:              Optional title to be added to the transform

        Returns:
            v                   Whatever the data that is requested (awkward arrays, etc.)
        '''
        # Check the call is legal for this datasource.
        a_func = cast(ast.Name, a.func)
        self.check_data_format_request(a_func.id)

        # Get the qastle string for this query
        q_str = self.generate_qastle(a)
        logging.getLogger(__name__).debug(f'Qastle string sent to servicex: {q_str}')

        # If only qastle is wanted, return here.
        if self.return_qastle:
            return q_str

        # Find the function we need to run against.
        if a_func.id in self._ds_map:
            name = self._ds_map[a_func.id]
        else:
            data_type = self._ds.first_supported_datatype(['parquet', 'root'])
            if data_type is not None and data_type in self._format_map:
                name = self._format_map[data_type]
            else:
                raise FuncADLServerException(f'Internal error - asked for {a_func.id} - but this dataset does not support it.')

        # Run ghe query for real!
        attr = getattr(self._ds, name)
        return await attr(q_str, title=title)


class ServiceXSourceCPPBase(ServiceXDatasetSourceBase):
    def __init__(self, sx: Union[ServiceXDataset, DatasetType], backend_name: str):
        '''Create a C++ backend data set source

        Args:
            sx (Union[ServiceXDataset, str]): The ServiceX dataset or dataset source.
            backend_name (str): The backend type, `xaod`, for example, for the ATLAS R21 xaod
        '''
        super().__init__(sx, backend_name)

        # Add the filename
        self.query_ast.args.append(ast.Str(s='bogus.root'))  # type: ignore


class ServiceXSourceXAOD(ServiceXSourceCPPBase):
    def __init__(self, sx: Union[ServiceXDataset, DatasetType], backend='xaod'):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__(sx, backend)


class ServiceXSourceCMSRun1AOD(ServiceXSourceCPPBase):
    def __init__(self, sx: Union[ServiceXDataset, DatasetType], backend='cms_run1_aod'):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__(sx, backend)


class ServiceXSourceUpROOT(ServiceXDatasetSourceBase):
    def __init__(self, sx: Union[ServiceXDataset, DatasetType], treename: str, backend_name='uproot'):
        '''
        Create a servicex dataset sequence from a servicex dataset
        '''
        super().__init__(sx, backend_name)

        # Modify the argument list in EventDataSset to include a dummy filename and
        # tree name
        self.query_ast.args.append(ast.Str(s='bogus.root'))  # type: ignore
        self.query_ast.args.append(ast.Str(s=treename))  # type: ignore
