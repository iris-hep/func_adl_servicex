import ast
from func_adl_servicex.ServiceX import ServiceXDatasetSourceBase
from qastle import python_ast_to_text_ast
import sys
from typing import Optional, cast

import pytest
from func_adl import ObjectStream
from servicex import ServiceXDataset
import awkward as ak

from func_adl_servicex import (
    FuncADLServerException,
    ServiceXSourceUpROOT,
    ServiceXSourceCMSRun1AOD,
    ServiceXSourceXAOD,
)


async def do_exe(a, title: Optional[str] = None):
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
    "Make sure that we cannot build the abstract base class"
    sx = mocker.MagicMock(spec=ServiceXDataset)
    with pytest.raises(Exception):
        ServiceXDatasetSourceBase(sx)  # type: ignore


def test_sx_uproot(async_mock):
    "Make sure we turn the execution into a call with an uproot"
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    a = ds.value(executor=do_exe)
    if sys.version_info < (3, 8):
        assert (
            ast.dump(a)
            == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Str(s='bogus.root'), Str(s='my_tree')], keywords=[])"
        )
    else:
        assert (
            ast.dump(a)
            == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Constant(value='bogus.root'), Constant(value='my_tree')], keywords=[])"
        )


def test_sx_uproot_root(async_mock):
    "Test a request for parquet files from an xAOD guy bombs"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = None
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: e.MET").AsROOTTTree(
        "junk.parquet", "another_tree", ["met"]
    )

    with pytest.raises(FuncADLServerException) as e:
        q.value()

    assert "not supported" in str(e.value)


def test_sx_uproot_parquet_one_col(async_mock):
    "Test a request for parquet files as parquet files works"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: e.MET").AsParquetFiles("junk.parquet", ["met"])

    q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Select(EventDataset('bogus.root', 'my_tree'), lambda e: e.MET), lambda x: {'met': x})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title=None,
    )


def test_sx_uproot_parquet_one_col_where(async_mock):
    "Test a request for parquet files as parquet files works, protected by a where"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = (
        ds.Select("lambda e: e.MET")
        .Where("lambda x: x > 10")
        .AsParquetFiles("junk.parquet", ["met"])
    )

    q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Where(Select(EventDataset('bogus.root', 'my_tree'), lambda e: e.MET), lambda x: x > 10), lambda x: {'met': x})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title=None,
    )


def test_sx_uproot_parquet_one_col_tuple(async_mock):
    "Test a request for parquet files as parquet files works"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: (e.MET, )").AsParquetFiles("junk.parquet", ["met"])

    q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Select(EventDataset('bogus.root', 'my_tree'), lambda e: (e.MET,)), lambda x: {'met': x[0]})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title=None,
    )


def test_sx_uproot_parquet_no_columns(async_mock):
    "Test a request for parquet files as parquet files works"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: {'MET': e.MET}").AsParquetFiles("junk.parquet")

    q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(EventDataset('bogus.root', 'my_tree'), lambda e: {'MET': e.MET})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title=None,
    )


def test_sx_uproot_parquet_multiple_columns(async_mock):
    "Test a request for parquet files as parquet files works"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: (e.MET, e.MET)").AsParquetFiles(
        "junk.parquet", ["met1", "met2"]
    )

    q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Select(EventDataset('bogus.root', 'my_tree'), lambda e: (e.MET, e.MET)), lambda x: {'met1': x[0], 'met2': x[1]})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title=None,
    )


def test_sx_uproot_default(async_mock):
    "Test a request for data as dict with no explicit output returns parquet files"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select(lambda e: e.MET).Select(
        lambda met: {
            "met": met,
        }
    )

    q.value()

    sx.get_data_parquet_async.assert_called_with(
        "(call Select (call Select (call EventDataset 'bogus.root' 'my_tree') (lambda (list e) (attr e 'MET'))) (lambda (list met) (dict (list 'met') (list met))))",
        title=None,
    )


def test_sx_uproot_parquet_title(async_mock):
    "Test a request for parquet files from an xAOD guy bombs"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: e.MET").AsParquetFiles("junk.parquet", ["met"])

    q.value(title="no way")

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Select(EventDataset('bogus.root', 'my_tree'), lambda e: e.MET), lambda x: {'met': x})"
            ).body[0],
        ).value
    )

    sx.get_data_parquet_async.assert_called_with(
        actual_call,
        title="no way",
    )


def test_sx_uproot_parquet_qastle(async_mock):
    "Test a request for parquet files from an xAOD guy bombs"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    ds.return_qastle = True
    q = ds.Select("lambda e: e.MET").AsParquetFiles("junk.parquet", ["met"])

    result = q.value()

    actual_call = python_ast_to_text_ast(
        cast(
            ast.Expr,
            ast.parse(
                "Select(Select(EventDataset('bogus.root', 'my_tree'), lambda e: e.MET), lambda x: {'met': x})"
            ).body[0],
        ).value
    )

    assert result == actual_call
    sx.get_data_parquet_async.assert_not_called()


def test_sx_uproot_awkward(async_mock):
    "Test a request for awkward data from uproot does proper request"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: e.MET").AsAwkwardArray(["met"])

    q.value()

    sx.get_data_awkward_async.assert_called_with(
        "(call Select (call EventDataset 'bogus.root' 'my_tree') (lambda (list e) (attr e 'MET')))",
        title=None,
    )


def test_sx_uproot_pandas(async_mock):
    "Test a request for awkward data from an xAOD guy bombs"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "parquet"
    ds = ServiceXSourceUpROOT(sx, "my_tree")
    q = ds.Select("lambda e: e.MET").AsPandasDF(["met"])

    q.value()

    sx.get_data_pandas_df_async.assert_called_with(
        "(call Select (call EventDataset 'bogus.root' 'my_tree') (lambda (list e) (attr e 'MET')))",
        title=None,
    )


def test_sx_xaod(async_mock):
    "Make sure we turn the execution into a call with an uproot"
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    a = ds.value(executor=do_exe)
    if sys.version_info < (3, 8):
        assert (
            ast.dump(a)
            == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Str(s='bogus.root')], keywords=[])"
        )
    else:
        assert (
            ast.dump(a)
            == "Call(func=Name(id='EventDataset', ctx=Load()), args=[Constant(value='bogus.root')], keywords=[])"
        )


def test_sx_xaod_parquet(async_mock):
    "Test a request for parquet files from an xAOD guy bombs"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = None
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsParquetFiles("junk.parquet", ["met"])

    with pytest.raises(FuncADLServerException) as e:
        q.value()

    assert "not supported" in str(e.value)


def test_sx_xaod_root(async_mock):
    "Test a request for root files from an xAOD guy"
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsROOTTTree("junk.root", "my_tree", ["met"])

    q.value()

    sx.get_data_rootfiles_async.assert_called_with(
        "(call ResultTTree (call Select (call EventDataset 'bogus.root') (lambda (list e) (attr e 'MET'))) (list 'met') 'my_tree' 'junk.root')",
        title=None,
    )


def test_sx_xaod_awkward(async_mock):
    "Test a request for awkward arrays from an xAOD backend"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray(["met"])

    q.value()

    sx.get_data_awkward_async.assert_called_with(
        "(call ResultTTree (call Select (call EventDataset 'bogus.root') (lambda (list e) (attr e 'MET'))) (list 'met') 'treeme' 'junk.root')",
        title=None,
    )


def test_sx_xaod_awkward_single_column(async_mock):
    "Test a request for awkward arrays from an xAOD backend, as column name"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray("met")

    q.value()

    sx.get_data_awkward_async.assert_called_with(
        "(call ResultTTree (call Select (call EventDataset 'bogus.root') (lambda (list e) (attr e 'MET'))) (list 'met') 'treeme' 'junk.root')",
        title=None,
    )


def test_sx_xaod_awkward_single_dict(async_mock):
    "Test a request for awkward arrays from an xAOD backend, as dict labelting"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: {'met': e.MET}").AsAwkwardArray()

    q.value()

    sx.get_data_awkward_async.assert_called_with(
        "(call Select (call EventDataset 'bogus.root') (lambda (list e) (dict (list 'met') (list (attr e 'MET')))))",
        title=None,
    )


def test_sx_xaod_awkward_no_columns(async_mock):
    "Test a request for awkward arrays from an xAOD backend"
    sx = async_mock(spec=ServiceXDataset)
    sx.get_data_awkward_async.return_value = ak.Array({"col1": [1, 2, 3]})
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray()

    q.value()

    sx.get_data_awkward_async.assert_called_with(
        "(call Select (call EventDataset 'bogus.root') (lambda (list e) (attr e 'MET')))",
        title=None,
    )


def test_sx_xaod_awkward_no_column_direct(async_mock):
    "Get the ak.Array directly with no dict access if we do not specify a column"
    sx = async_mock(spec=ServiceXDataset)
    sx.get_data_awkward_async.return_value = ak.Array({"col1": [1, 2, 3]})
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray()

    r = q.value()
    assert str(ak.type(r)) == "3 * int64"


def test_sx_xaod_awkward_col_name_direct(async_mock):
    "Get the ak.Array directly with no dict access if we do not specify a column"
    sx = async_mock(spec=ServiceXDataset)
    sx.get_data_awkward_async.return_value = ak.Array({"col1": [1, 2, 3]})
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsAwkwardArray("col1")

    r = q.value()
    assert str(ak.type(r)) == '3 * {"col1": int64}'


def test_sx_xaod_pandas(async_mock):
    "Test a request for awkward arrays from an xAOD backend"
    sx = async_mock(spec=ServiceXDataset)
    sx.first_supported_datatype.return_value = "root-file"
    ds = ServiceXSourceXAOD(sx)
    q = ds.Select("lambda e: e.MET").AsPandasDF(["met"])

    q.value()

    sx.get_data_pandas_df_async.assert_called_with(
        "(call ResultTTree (call Select (call EventDataset 'bogus.root') (lambda (list e) (attr e 'MET'))) (list 'met') 'treeme' 'junk.root')",
        title=None,
    )


def test_ctor_xaod(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceXAOD("did_1221")
    call.assert_called_with("did_1221", backend_name="xaod")


def test_ctor_xaod_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceXAOD("did_1221", backend="myleftfoot")
    call.assert_called_with("did_1221", backend_name="myleftfoot")


def test_ctor_cms(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceCMSRun1AOD("did_1221")
    call.assert_called_with("did_1221", backend_name="cms_run1_aod")


def test_ctor_cms_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceCMSRun1AOD("did_1221", backend="fork")
    call.assert_called_with("did_1221", backend_name="fork")


def test_ctor_uproot(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceUpROOT("did_1221", "a_tree")
    call.assert_called_with("did_1221", backend_name="uproot")


def test_ctor_uproot_alternate_backend(mocker):
    call = mocker.MagicMock(return_value=mocker.MagicMock(spec=ServiceXDataset))
    mocker.patch("func_adl_servicex.ServiceX.ServiceXDataset", call)
    ServiceXSourceUpROOT("did_1221", "a_tree", backend_name="myleftfoot")
    call.assert_called_with("did_1221", backend_name="myleftfoot")


def test_bad_wrong_call_name_right_args(async_mock):
    "A call needs to be vs a Name node, not something else?"
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    next = ast.Call(
        func=ast.Name(id="ResultBogus", ctx=ast.Load()),
        args=[ds.query_ast, ast.Name(id="cos", ctx=ast.Load())],
    )

    with pytest.raises(FuncADLServerException) as e:
        ObjectStream(next).value()

    assert "ResultBogus" in str(e.value)


def test_bad_wrong_call_name(async_mock):
    "A call needs to be vs a Name node, not something else?"
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)
    next = ast.Call(func=ast.Name(id="ResultBogus"), args=[ds.query_ast])

    with pytest.raises(FuncADLServerException) as e:
        ObjectStream(next).value()

    assert "ResultBogus" in str(e.value)
