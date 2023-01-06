from op_netsuite_fetcher.constants import CONFIG, ConfigFields, BLOB_CONTAINER_NAME, BLOB_NAME
from op_netsuite_fetcher.common.helpers import connect_blob
from op_netsuite_fetcher.conf import get_logger
from abc import ABC, abstractmethod
from typing import Any
import requests
import json
import uuid
import os


logger = get_logger()


class Base(ABC):
    """
    Base class to derive all other classes' extractor,
    fetcher and dispatcher from
    """

    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.config_template = None
        self.object_type = None  # Member variable to store current object type
        self.kwargs = kwargs
        self.logger = logger
        self.data = None
        self.meta = {}  # Member variable to store meta data file

    def read_config(self) -> Any:
        try:
            if CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'blob':
                self.logger.info("Using config files from azure blob")
                with requests.get(
                    self.kwargs.get('config_file_path')
                ) as config_file:
                    self.config_template = json.loads(config_file.text)
            elif CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'local':
                self.logger.info("Using config files from local directory")
                with open(
                    self.kwargs.get('config_file_path'), 'r'
                ) as config_file:
                    self.config_template = json.load(config_file)
            else:
                self.logger.error("You need to pass CONFIG_FILE_LOCATION")
                raise Exception("Couldnot find type for CONFIG_FILE_LOCATION")

        except Exception as ex:
            self.logger.error(
                "Error occured during reading config files",
                exc_info=True
            )
            raise ex

    def write(self, **kwargs) -> Any:
        # Check for potential errors
        if not self.data:
            return logger.info("No data to save")

        if self.data == "":
            return logger.info("Data is empty after extraction")

        if 'data_file_dir' in kwargs.keys():
            data_file_dir = kwargs.get('data_file_dir')
        else:
            raise NotImplementedError(
                "Implementation for 'write' method not present"
            )

        if data_file_dir is None:
            raise Exception("Data file path is None")

        if CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'blob':
            blob_service_client = connect_blob()
            if not blob_service_client:
                raise Exception(
                    "Data file path does not exist or is inaccessible"
                )

        elif CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'local':
            if not os.path.exists(data_file_dir):
                raise Exception(
                    "Data file path does not exist or is inaccessible"
                )
            if not os.path.isdir(data_file_dir):
                raise Exception("Data file path is not a directory")

        # Write the vendor data to file
        self.logger.info("Saving fetched data to a file/blob")
        try:
            file_name = uuid.uuid1()
            
            if CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'local':
                data_file_path = os.path.join(
                    data_file_dir, str(file_name) + ".json"
                )
                with open(data_file_path, 'w') as outfile:
                    outfile.write(json.dumps(self.data))

            elif CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'blob':
                data_file_path = os.path.join(
                    str(file_name) + ".json"
                )
                
                container = CONFIG.get(BLOB_CONTAINER_NAME) + "/" + CONFIG.get(BLOB_NAME) + "/" + data_file_dir
                blob_client = blob_service_client.get_blob_client(
                    container=container,
                    blob=data_file_path
                )
                blob_client.upload_blob(json.dumps(self.data))

            self.meta[self.object_type.value.lower() + "_data_file_path"] = data_file_path
            return self
        except Exception as ex:
            raise Exception(ex)
