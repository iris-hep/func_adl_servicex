import ast
import sys

import pytest
from func_adl import ObjectStream
from servicex import ServiceXDataset

from func_adl_servicex import (FuncADLServerException,
                                        ServiceXSourceCMSRun1AOD,
                                        ServiceXSourceUpROOT,
                                        ServiceXSourceXAOD)


async def do_exe(a):
    return a


@pytest.fixture
def async_mock(mocker):
    import sys
    if sys.version_info[1] <= 7:
        import asyncmock
        return asyncmock.MagicMock
    else:
        return mocker.MagicMock


def test_sx_abs(mocker):
    'Make sure that we cannot build the abstract base class'
    sx = mocker.MagicMock(spec=ServiceXDataset)
    with pytest.raises(Exception):
        ServiceXDatasetSourceBase(sx)  # type: ignore


def test_sx_uproot(async_mock):
    'Make sure we turn the execution into a call with an uproot'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, 'my_tree')
    a = ds.value(executor=do_exe)
    if sys.version_info < (3, 8):
        assert ast.dump(a) == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Str(s='my_tree')], keywords=[])"
    else:
        assert ast.dump(a) == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Constant(value='my_tree')], keywords=[])"


def test_sx_uproot_root(async_mock):
    'Test a request for parquet files from an xAOD guy bombs'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, 'my_tree')
    q = ds.Select("lambda e: e.MET").AsROOTTTree('junk.parquet', 'another_tree', ['met'])

    with pytest.raises(FuncADLServerException) as e:
        q.value()

    assert 'not supported' in str(e.value)


def test_sx_uproot_parquet(async_mock):
    'Test a request for parquet files from an xAOD guy bombs'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, 'my_tree')
    q = ds.Select("lambda e: e.MET").AsParquetFiles('junk.parquet', ['met'])

    q.value()

    sx.get_data_parquet_async.assert_called_with("(Select (call EventDataset 'my_tree') (lambda (list e) (attr e 'MET')))")


def test_sx_uproot_awkward(async_mock):
    'Test a request for awkward data from an xAOD guy bombs'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, 'my_tree')
    q = ds.Select("lambda e: e.MET").AsAwkwardArray(['met'])

    q.value()

    sx.get_data_awkward_async.assert_called_with("(Select (call EventDataset 'my_tree') (lambda (list e) (attr e 'MET')))")


def test_sx_uproot_pandas(async_mock):
    'Test a request for awkward data from an xAOD guy bombs'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, 'my_tree')
    q = ds.Select("lambda e: e.MET").AsPandasDF(['met'])

    q.value()

    sx.get_data_pandas_df_async.assert_called_with("(Select (call EventDataset 'my_tree') (lambda (list e) (attr e 'MET')))")


def test_sx_xaod(async_mock):
    'Make sure we turn the execution into a call with an uproot'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    a = ds.value(executor=do_exe)
    assert ast.dump(a) == "Call(func=Name(id='EventDataset', ctx=Load()), args=[], keywords=[])"


def test_sx_xaod_parquet(async_mock):
    'Test a request for parquet files from an xAOD guy bombs'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsParquetFiles('junk.parquet', ['met'])

    with pytest.raises(FuncADLServerException) as e:
        q.value()

    assert 'not supported' in str(e.value)


def test_sx_xaod_root(async_mock):
    'Test a request for root files from an xAOD guy'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsROOTTTree('junk.root', 'my_tree', ['met'])

    q.value()

    sx.get_data_rootfiles_async.assert_called_with("(call ResultTTree (call Select (call EventDataset) (lambda (list e) (attr e 'MET'))) (list 'met') 'my_tree' 'junk.root')")


def test_sx_xaod_awkward(async_mock):
    'Test a request for awkward arrays from an xAOD backend'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray(['met'])

    q.value()

    sx.get_data_awkward_async.assert_called_with("(call ResultTTree (call Select (call EventDataset) (lambda (list e) (attr e 'MET'))) (list 'met') 'treeme' 'file.root')")


def test_sx_xaod_pandas(async_mock):
    'Test a request for awkward arrays from an xAOD backend'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsPandasDF(['met'])

    q.value()

    sx.get_data_pandas_df_async.assert_called_with("(call ResultTTree (call Select (call EventDataset) (lambda (list e) (attr e 'MET'))) (list 'met') 'treeme' 'file.root')")


def test_ctor_xaod(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceXAOD('did_1221')
    call.assert_called_with('did_1221', backend_type='xaod')


def test_ctor_xaod_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceXAOD('did_1221', backend='myleftfoot')
    call.assert_called_with('did_1221', backend_type='myleftfoot')


def test_ctor_cms(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceCMSRun1AOD('did_1221')
    call.assert_called_with('did_1221', backend_type='cms_run1_aod')


def test_ctor_cms_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceCMSRun1AOD('did_1221', backend='fork')
    call.assert_called_with('did_1221', backend_type='fork')


def test_ctor_uproot(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceUpROOT('did_1221', 'a_tree')
    call.assert_called_with('did_1221', backend_type='uproot')


def test_ctor_uproot_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch('func_adl_servicex.ServiceX.ServiceXDataset', call)
    ServiceXSourceUpROOT('did_1221', 'a_tree', backend='myleftfoot')
    call.assert_called_with('did_1221', backend_type='myleftfoot')


def test_bad_wrong_call_name_right_args(async_mock):
    'A call needs to be vs a Name node, not something else?'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    next = ast.Call(
        func=ast.Name(id='ResultBogus', ctx=ast.Load()),
        args=[ds.query_ast, ast.Name(id='cos', ctx=ast.Load())])

    with pytest.raises(FuncADLServerException) as e:
        ObjectStream(next) \
            .value()

    assert "ResultBogus" in str(e.value)


def test_bad_wrong_call_name(async_mock):
    'A call needs to be vs a Name node, not something else?'
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    next = ast.Call(func=ast.Name(id='ResultBogus'), args=[ds.query_ast])

    with pytest.raises(FuncADLServerException) as e:
        ObjectStream(next) \
            .value()

    assert "ResultBogus" in str(e.value)
