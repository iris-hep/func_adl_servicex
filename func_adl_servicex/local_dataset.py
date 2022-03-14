from abc import ABC, abstractmethod
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import aiohttp
from func_adl_xAOD.atlas.xaod import xAODDataset
from func_adl_xAOD.cms.aod import CMSRun1AODDataset
from func_adl_xAOD.common.local_dataset import LocalDataset
from qastle import text_ast_to_python_ast
from servicex import ServiceXDataset
from servicex.utils import ServiceXUnknownDataRequestID

from .ServiceX import ServiceXSourceCPPBase


class _local_file_copier:
    def __init__(self):
        self._request_info_dict: Dict[str, List[Path]] = {}

    def associate_file(self, request_id: str, files: List[Path]):
        '''These are the files that should be returned when the query

        Args:
            request_id (str): The request id
            files (List[str]): The files to attach to this request id
        '''
        self._request_info_dict[request_id] = files

    def get_files(self, request_id: str) -> List[str]:
        '''When the run has finished, return the file.

        Args:
            request_id (str): The request ID we are doing this for

        Returns:
            List[str]: List of the files
        '''
        if request_id not in self._request_info_dict:
            raise ServiceXUnknownDataRequestID(f'Files for request {request_id} not found')

        return [f.name for f in self._request_info_dict[request_id]]

    async def download_file(self,
                            request_id: str,
                            bucket_name: str,
                            output_file: Path) -> None:
        'Copy the files from the request id to the appropriate output location'
        # Find the proper file to copy over from our list of files.
        all_files = [f for f in self._request_info_dict[request_id] if f.name == bucket_name]
        all_files[0].rename(output_file)


class _sx_local_file_minio_factory:
    def __init__(self):
        self._local = _local_file_copier()

    def associate_file(self, request_id: str, files: List[Path]):
        'Pass the association down the line'
        self._local.associate_file(request_id, files)

    def from_best(self, _: Optional[Dict[str, str]] = None):
        'Since this is never "complex", we can just return a one-off'
        return self._local


class _sx_local_file_adaptor:
    def __init__(self, ds: LocalDataset, minio: _sx_local_file_minio_factory):
        '''Create a ServiceXAdaptor that will run the local file accessor.
        '''
        self._ds = ds
        self._minio = minio

    async def submit_query(self, _: aiohttp.ClientSession,
                           json_query: Dict[str, str]) -> Dict[str, str]:
        '''Kick off the docker run, and then return, right away, a response
        that will mock the ServiceX backend.

        Args:
            _ (aiohttp.ClientSession): The http context (ignored)
            json_query (Dict[str, str]): Contains the info on the query

        Returns:
            Dict[str, str]: The reply from ServiceX
        '''
        # Generate a new unique ID for this query
        request_id = str(uuid.uuid4())

        # Next, run the thing, and wait for it to finish
        query = json_query['selection']
        title = json_query['title'] if 'title' in json_query else ''
        file_list = await self._ds.execute_result_async(
            text_ast_to_python_ast(query).body[0].value,
            title
        )

        self._minio.associate_file(request_id, file_list)

        # Return the info needed from the submit!
        return {
            'request_id': request_id,
        }

    async def get_query_status(self, _: aiohttp.ClientSession,
                               request_id: str) -> Dict[str, str]:
        '''Returns the full query information from the endpoint.
        Args:
            client (aiohttp.ClientSession): Client session on which to make the request.
            request_id (str): The request id to return the tranform status
        Raises:
            ServiceXException: If we fail to find the information.
        Returns:
            Dict[str, str]: The JSON dictionary of information returned from ServiceX
        '''
        return {
            'request_id': request_id,
            'files-remaining': '0',
            'files-skipped': '0',
            'files-processed': '1',
        }

    async def get_transform_status(self, client: aiohttp.ClientSession, request_id: str) -> \
            Tuple[Optional[int], int, Optional[int]]:
        return (0, 1, 0)


T = TypeVar('T')


class SXLocalCPP(ServiceXSourceCPPBase[T], ABC):
    def __init__(self, files: Union[str, List[str], Path, List[Path]],
                 docker_image: Optional[str] = None,
                 docker_tag: Optional[str] = None,
                 item_type: type = Any):
        '''A local ServiceX-like dataset. Used for the C++ backends

        NOTE: This version of is not suitable for running large numbers of files or
        datasets simultantiously on your local machine! Everything is run serially.

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
        '''

        # Create the local dataset
        ds = self._create_dataset(files, docker_image, docker_tag)

        # To create the ServiceXDataset
        minio_local = _sx_local_file_minio_factory()
        sx_ds = ServiceXDataset(
            'local_dataset',
            backend_name=self._get_backend_type(),
            servicex_adaptor=_sx_local_file_adaptor(ds, minio_local),  # type: ignore
            minio_adaptor=minio_local,  # type: ignore
            status_callback_factory=None,
        )

        # And create our home body!
        super().__init__(sx_ds, self._get_backend_type(), item_type)

    @classmethod
    @abstractmethod
    def _create_dataset(cls, files: Union[str, List[str], Path, List[Path]],
                        docker_image: Optional[str] = None,
                        docker_tag: Optional[str] = None) -> LocalDataset:
        '''Create a dataset that can be used with the C++ backends

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
            docker_image (Optional[str]): The docker image to use.
            docker_tag (Optional[str]): The docker tag to use.
        '''

    @classmethod
    @abstractmethod
    def _get_backend_type(cls) -> str:
        'Return the backend type/name string'


U = TypeVar('U')


class SXLocalxAOD(SXLocalCPP[U]):
    @classmethod
    def _create_dataset(cls, files: Union[str, List[str], Path, List[Path]],
                        docker_image: Optional[str] = None,
                        docker_tag: Optional[str] = None) -> LocalDataset:
        '''Create a dataset that can be used with the C++ backends

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
            docker_image (Optional[str]): The docker image to use.
            docker_tag (Optional[str]): The docker tag to use.
        '''
        extra_args = {}
        if docker_image is not None:
            extra_args['docker_image'] = docker_image
        if docker_tag is not None:
            extra_args['docker_tag'] = docker_tag
        return xAODDataset(files, **extra_args)

    @classmethod
    def _get_backend_type(cls) -> str:
        'Return code for a xaod backend'
        return 'xaod'

    def __init__(self, files: Union[str, List[str], Path, List[Path]],
                 docker_image: Optional[str] = None,
                 docker_tag: Optional[str] = None,
                 item_type: type = Any):
        '''A local ServiceX-like dataset. Will run locally, in docker, synchronously.

        NOTE: This version of is not suitable for running large numbers of files or
        datasets simultantiously on your local machine! Everything is run serially.

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
        '''
        super().__init__(files, docker_image, docker_tag, item_type)


V = TypeVar('V')


class SXLocalCMSRun1AOD(SXLocalCPP[V]):
    @classmethod
    def _create_dataset(cls, files: Union[str, List[str], Path, List[Path]],
                        docker_image: Optional[str] = None,
                        docker_tag: Optional[str] = None) -> LocalDataset:
        '''Create a dataset that can be used with the CMSRun1 backend

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
            docker_image (Optional[str]): The docker image to use.
            docker_tag (Optional[str]): The docker tag to use.
        '''
        extra_args = {}
        if docker_image is not None:
            extra_args['docker_image'] = docker_image
        if docker_tag is not None:
            extra_args['docker_tag'] = docker_tag
        return CMSRun1AODDataset(files, **extra_args)

    @classmethod
    def _get_backend_type(cls) -> str:
        'Return code for a cms_run1_aod backend'
        return 'cms_run1_aod'

    def __init__(self, files: Union[str, List[str], Path, List[Path]],
                 docker_image: Optional[str] = None,
                 docker_tag: Optional[str] = None,
                 item_type: type = Any):
        '''A local ServiceX-like dataset. Will run locally, in docker, synchronously.

        NOTE: This version of is not suitable for running large numbers of files or
        datasets simultantiously on your local machine! Everything is run serially.

        Args:
            files (Union[str, List[str], Path, List[Path]]): List of files to run on.
        '''
        super().__init__(files, docker_image, docker_tag, item_type)
