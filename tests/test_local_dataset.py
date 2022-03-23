import ast
import sys
import tempfile
from pathlib import Path

import pytest
from servicex import ignore_cache

# If the CI hasn't loaded python on whales, then we aren't using docker
python_on_whales = pytest.importorskip("python_on_whales")


@pytest.fixture()
def xAODDataset_mock(mocker):
    from func_adl_xAOD.atlas.xaod import xAODDataset
    xds = mocker.MagicMock(spec=xAODDataset)
    ctor = mocker.MagicMock(return_value=xds)

    mocker.patch('func_adl_servicex.local_dataset.xAODDataset', ctor)

    # The file will need to actually exist...
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_file = (Path(tmpdir) / 'Analysis.root')
        analysis_file.touch()
        if sys.version_info >= (3, 8):
            xds.execute_result_async.return_value = [analysis_file]
        else:
            import asyncio
            f = asyncio.Future()
            f.set_result([analysis_file])
            xds.execute_result_async.return_value = f
        yield ctor, xds


@pytest.fixture()
def CMSRun1AODDataset_mock(mocker):
    from func_adl_xAOD.cms.aod import CMSRun1AODDataset
    xds = mocker.MagicMock(spec=CMSRun1AODDataset)
    ctor = mocker.MagicMock(return_value=xds)

    mocker.patch('func_adl_servicex.local_dataset.CMSRun1AODDataset', ctor)

    # The file will need to actually exist...
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_file = (Path(tmpdir) / 'Analysis.root')
        analysis_file.touch()
        if sys.version_info >= (3, 8):
            xds.execute_result_async.return_value = [analysis_file]
        else:
            import asyncio
            f = asyncio.Future()
            f.set_result([analysis_file])
            xds.execute_result_async.return_value = f
        yield ctor, xds


@pytest.fixture(autouse=True)
def ignore_cache_for_test():
    with ignore_cache():
        yield


def test_ctor_cms(CMSRun1AODDataset_mock):
    'Make sure arguments are passed in correctly'
    from func_adl_servicex import SXLocalCMSRun1AOD
    SXLocalCMSRun1AOD('junk.root')

    CMSRun1AODDataset_mock[0].assert_called_once_with('junk.root')


def test_good_call_cms(CMSRun1AODDataset_mock):
    'Make sure the call works'
    from func_adl_servicex import SXLocalCMSRun1AOD
    v = (SXLocalCMSRun1AOD('my_dataset.root')
         .SelectMany(lambda e: e.Jets('AntiKt4'))
         .Select(lambda j: j.pt())
         .value()
         )

    assert CMSRun1AODDataset_mock[1].execute_result_async.call_count == 1
    query = CMSRun1AODDataset_mock[1].execute_result_async.call_args[0][0]
    assert "pt" in ast.dump(query)
    assert "Module" not in ast.dump(query)
    assert len(CMSRun1AODDataset_mock[1].execute_result_async.call_args[0][1]) == 0

    assert len(v) == 1
    assert isinstance(v[0], Path)


def test_docker_image_setting_cms(CMSRun1AODDataset_mock):
    'Make sure we can set docker tag and image correctly'
    from func_adl_servicex import SXLocalCMSRun1AOD
    (SXLocalCMSRun1AOD('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
     .SelectMany(lambda e: e.Jets('AntiKt4'))
     .Select(lambda j: j.pt())
     .value()
     )
    CMSRun1AODDataset_mock[0].assert_called_once_with('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')


def test_ctor_xaod(xAODDataset_mock):
    'Make sure arguments are passed in correctly'
    from func_adl_servicex import SXLocalxAOD
    SXLocalxAOD('junk.root')

    xAODDataset_mock[0].assert_called_once_with('junk.root')


def test_good_call_xaod(xAODDataset_mock):
    'Make sure the call works'
    from func_adl_servicex import SXLocalxAOD
    v = (SXLocalxAOD('my_dataset.root')
         .SelectMany(lambda e: e.Jets('AntiKt4'))
         .Select(lambda j: j.pt())
         .value()
         )

    assert xAODDataset_mock[1].execute_result_async.call_count == 1
    query = xAODDataset_mock[1].execute_result_async.call_args[0][0]
    assert "pt" in ast.dump(query)
    assert len(xAODDataset_mock[1].execute_result_async.call_args[0][1]) == 0

    assert len(v) == 1
    assert isinstance(v[0], Path)


def test_docker_image_setting_xaod(xAODDataset_mock):
    'Make sure we can set docker tag and image correctly'
    from func_adl_servicex import SXLocalxAOD
    (SXLocalxAOD('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
     .SelectMany(lambda e: e.Jets('AntiKt4'))
     .Select(lambda j: j.pt())
     .value()
     )
    xAODDataset_mock[0].assert_called_once_with('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
