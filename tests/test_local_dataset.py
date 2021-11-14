import ast
import tempfile
from pathlib import Path

import pytest
from func_adl_servicex import SXLocalxAOD
from func_adl_xAOD.atlas.xaod import xAODDataset
from servicex import ignore_cache

from func_adl_servicex.local_dataset import SXLocalCMSRun1AOD


@pytest.fixture()
def xAODDataset_mock(mocker):
    xds = mocker.MagicMock(spec=xAODDataset)
    ctor = mocker.MagicMock(return_value=xds)

    mocker.patch('func_adl_servicex.local_dataset.xAODDataset', ctor)

    # The file will need to actually exist...
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_file = (Path(tmpdir) / 'Analysis.root')
        analysis_file.touch()
        xds.execute_result_async.return_value = [analysis_file]
        yield ctor, xds


@pytest.fixture()
def CMSRun1AODDataset_mock(mocker):
    xds = mocker.MagicMock(spec=xAODDataset)
    ctor = mocker.MagicMock(return_value=xds)

    mocker.patch('func_adl_servicex.local_dataset.CMSRun1AODDataset', ctor)

    # The file will need to actually exist...
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_file = (Path(tmpdir) / 'Analysis.root')
        analysis_file.touch()
        xds.execute_result_async.return_value = [analysis_file]
        yield ctor, xds


@pytest.fixture(autouse=True)
def ignore_cache_for_test():
    with ignore_cache():
        yield


def test_ctor_cms(CMSRun1AODDataset_mock):
    'Make sure arguments are passed in correctly'
    SXLocalCMSRun1AOD('junk.root')

    CMSRun1AODDataset_mock[0].assert_called_once_with('junk.root')


def test_good_call_cms(CMSRun1AODDataset_mock):
    'Make sure the call works'
    v = (SXLocalCMSRun1AOD('my_dataset.root')
         .SelectMany(lambda e: e.Jets('AntiKt4'))
         .Select(lambda j: j.pt())
         .value()
         )

    assert CMSRun1AODDataset_mock[1].execute_result_async.call_count == 1
    query = CMSRun1AODDataset_mock[1].execute_result_async.call_args[0][0]
    assert "pt" in ast.dump(query)
    assert len(CMSRun1AODDataset_mock[1].execute_result_async.call_args[0][1]) == 0

    assert len(v) == 1
    assert isinstance(v[0], Path)


def test_docker_image_setting_cms(CMSRun1AODDataset_mock):
    'Make sure we can set docker tag and image correctly'
    (SXLocalCMSRun1AOD('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
     .SelectMany(lambda e: e.Jets('AntiKt4'))
     .Select(lambda j: j.pt())
     .value()
     )
    CMSRun1AODDataset_mock[0].assert_called_once_with('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')


def test_ctor_xaod(xAODDataset_mock):
    'Make sure arguments are passed in correctly'
    SXLocalxAOD('junk.root')

    xAODDataset_mock[0].assert_called_once_with('junk.root')


def test_good_call_xaod(xAODDataset_mock):
    'Make sure the call works'
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
    (SXLocalxAOD('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
     .SelectMany(lambda e: e.Jets('AntiKt4'))
     .Select(lambda j: j.pt())
     .value()
     )
    xAODDataset_mock[0].assert_called_once_with('my_dataset.root', docker_image='fork/forky', docker_tag='mc_fork_face')
