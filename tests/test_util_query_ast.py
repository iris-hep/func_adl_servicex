
import pytest
from servicex import ServiceXDataset
from func_adl_servicex.ServiceX import ServiceXSourceXAOD
from func_adl_servicex.util_query_ast import has_col_names


@pytest.fixture
def async_mock(mocker):
    import sys
    if sys.version_info[1] <= 7:
        import asyncmock
        return asyncmock.MagicMock
    else:
        return mocker.MagicMock


def test_col_specified(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    query = (
        ds
        .Select(lambda e: {
            'col1': e.met
        })
        .AsAwkwardArray()
    )
    assert has_col_names(query.query_ast)


def test_col_specified_in_func(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    def func(e):
        return {'col1': e.met}

    query = (
        ds
        .Select(func)
        .AsAwkwardArray()
    )
    assert has_col_names(query.query_ast)


def test_col_specified_selectmany(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    query = (
        ds
        .SelectMany(lambda e: {
            'col1': e.met
        })
        .AsAwkwardArray()
    )
    assert has_col_names(query.query_ast)


def test_col_specified_in_awk(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    query = (
        ds
        .Select(lambda e: {
            e.met
        })
        .AsAwkwardArray('col1')
    )
    assert has_col_names(query.query_ast)


def test_col_not_specified(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    query = (
        ds
        .Select(lambda e: e.met)
    )
    assert not has_col_names(query.query_ast)


def test_col_not_specified_with_AWK(async_mock):
    sx = async_mock(spec=ServiceXDataset)
    ds = ServiceXSourceXAOD(sx)

    query = (
        ds
        .Select(lambda e: e.met)
        .AsAwkwardArray()
    )
    assert not has_col_names(query.query_ast)
