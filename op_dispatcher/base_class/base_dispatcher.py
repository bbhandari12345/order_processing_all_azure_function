from typing import Any
from abc import abstractmethod
from op_dispatcher.base_class.base import Base, requests, json
from op_dispatcher.constants import ObjectType, CONFIG, ConfigFields, EXTRACTOR_WRITE_PATH


class BaseDispatcher(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.DISPATCHER

    def read_data(self) -> Any:
        dir_path = EXTRACTOR_WRITE_PATH + "/" + self.kwargs.get('extractor_file_path')

        try:
            if CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'blob':
                self.logger.info("Reading files from azure blob")
                with requests.get(
                    CONFIG.get('CONFIG_DIRECTORY') + dir_path
                ) as file:
                    self.data = json.loads(file.text)
            elif CONFIG.get(ConfigFields.CONFIG_FILE_TYPE.value) == 'local':
                self.logger.info("Reading files from local directory")
                with open(
                    dir_path, 'r'
                ) as file:
                    self.data = json.load(file)
            else:
                self.logger.error("You need to pass fetcher_file_path")
                raise Exception("Couldnot find fetcher_file_path")

        except Exception as ex:
            self.logger.error(
                "Error occured during reading fetched files",
                exc_info=True
            )
            raise ex

    @abstractmethod
    def loader(self) -> Any:
        pass

    @abstractmethod
    def db_dispatcher(self) -> Any:
        pass

    def execute(self) -> Any:
        return self.read_transformed().loader().db_dispatcher()
