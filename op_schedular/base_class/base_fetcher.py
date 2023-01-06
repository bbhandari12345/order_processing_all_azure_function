from op_schedular.base_class.base import Base
from typing import Any
from abc import abstractmethod
from op_schedular.constants import ObjectType


class BaseFetcher(Base):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.object_type = ObjectType.FETCHER

    @abstractmethod
    def fetch_config(self) -> Any:
        pass

    @abstractmethod
    def extractor(self) -> Any:
        pass

    def execute(self) -> Any:
        return self.fetch_config().extractor().write(
            data_file_dir=self.kwargs['fetcher_write_path']
        )


class BaseNetsuiteFetcher(BaseFetcher):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @abstractmethod
    def transformer(self) -> Any:
        pass

    @abstractmethod
    def loader(self) -> Any:
        pass
    
    @abstractmethod
    def updater(self) -> Any:
        pass

    def execute(self) -> Any:
        return self.fetch_config().extractor().transformer().loader().updater()