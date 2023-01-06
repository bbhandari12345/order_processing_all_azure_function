"""
ORM base class for performing MRC database actions
"""

from marshmallow import fields, ValidationError, EXCLUDE
from op_schedular.common.orm_handler.common_orm import update_data, upsert_bulk_data
from abc import ABC, abstractmethod
from op_schedular.utils.logger import tracelog
from op_schedular.conf import get_logger
from typing import Any


logger = get_logger()


class VBOrmBase(ABC):
    """Base Class to perform db actions for the given table.

    Along with dynamic schema validation and dump for data in and out.
    This class performs saveCRUD operations
    :params src_data: dict or list of dict. Data to be loaded on schema for
                        validation other operations.
    :type src_data: List[dict] or dict
    """

    def __init__(self, src_data=None):
        """Perform sql_db actions for the given table.

        With dynamic schema validation and dump for data in and out. This class
        performs save CRUD operations in sql_db.

        :params src_data: dict or list of dict. Data to be loaded on schema for validation
                            other operations.
        :type src_data: List[dict] or dict
        """
        self.src_data = src_data

        self.loaded_data = None
        self.dumped_data = None

        self.is_loaded = False
        self.is_dumped = False

    @property
    @classmethod
    @abstractmethod
    def __table_name__(cls):
        """Class attribute for tablename."""
        pass

    @property
    @classmethod
    @abstractmethod
    def __schema__(cls):
        """Class attribute for table schema object."""
        pass

    @classmethod
    def get_schema(cls):
        """Return schema used in class.

        :returns: Table Schema
        :rtype: marshmallow.schema
        """
        return cls.__schema__

    @classmethod
    def get_table(cls):
        """Return tablename used in class.

        :returns: tablename
        :rtype: str
        """
        return cls.__table_name__

    @classmethod
    def get_dump_only_fields(cls) -> list:
        """Return schema fields defined as dump only.

        :returns: list of dump only fields name.
        :rtype:list
        """
        dump_only_fields = []
        declared_fields = getattr(cls.get_schema(), 'declared_fields')  # noqa: B009
        for key, value in declared_fields.items():
            if not isinstance(value, fields.Method):
                if getattr(value, 'dump_only'):  # noqa: B009
                    dump_only_fields.append(key)

        return dump_only_fields

    def check_src_data(self):
        """Check if src data loaded or not.

        :raises ValueError: Raised when src_data is None.
        """
        if self.src_data is None:
            raise ValueError('src_data must not be None.')

    def load(self, unknown=EXCLUDE, **kwargs):
        """Load src data to given schema along followed by validation.

        :params kwargs: keyword arguments supplied to marshmallow schema load method.
        :type kwargs: dict
        :returns: loaded dict
        :rtype: dict
        """
        self.check_src_data()

        logger.info(f'Loading data to {self.get_table()} schema.')
        logger.debug(f'Data for loading: {self.src_data}')
        self.is_loaded = True
        try:
            if isinstance(self.src_data, list):
                logger.debug('Got a list of data. Preparing to call schema load with many=True')
                self.loaded_data = self.get_schema().load(self.src_data, unknown=unknown, many=True, **kwargs)

            if isinstance(self.src_data, dict):
                logger.debug('Got a single dict data. Preparing to call schema load with many=False')
                self.loaded_data = self.get_schema().load(self.src_data, unknown=unknown, many=False, **kwargs)
        except ValidationError as err:
            logger.error("Encountered marhsmallow validation error", exc_info=True)
            logger.error(err)
            raise err

        return self.loaded_data

    def dump(self, **kwargs):
        """Dump schema data after proper schema build.

        :params kwargs: keyword arguments supplied to marshmallow schema dump method.
        :type kwargs: dict
        :returns: dynamically created schema dict list of dict values or single dict.
        :rtype: list, dict

        :raises Exception: Raised when marshmallow dump raises any exception.
        """
        self.check_src_data()

        logger.info(f'Dumping data of {self.get_table()}  schema.')
        self.is_dumped = True
        try:
            if isinstance(self.src_data, list):
                logger.debug('Got a list of data. Preparing to calll schema load with many=True')
                self.dumped_data = self.get_schema().dump(self.src_data, many=True, **kwargs)

            if isinstance(self.src_data, dict):
                logger.debug('Got a single dict data. Preparing to calll schema load with many=False')
                self.dumped_data = self.get_schema().dump(self.src_data, many=False, **kwargs)

        except Exception as e:
            logger.error('Error in data dump', exc_info=True)
            raise e
        return self.dumped_data


    @tracelog(logger)
    def update(self, row_value: Any = None, identifier: str = 'id'):
        """Update `src_data` in database.

        :params row_value: where clause filter value.
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`.
        :type identifier:str

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk insert isn't implemented.

        :raises ValueError: Raised when update() is called before loading or dumping data.
        """
        self.check_src_data()
        if isinstance(self.src_data, list) and len(self.src_data) > 1:
            logger.warning('Update for Multiple instances not implemented.')
            raise NotImplementedError('Update for Multiple instances not implemented yet.')

        if self.is_loaded is False and self.is_dumped is False:
            logger.critical('Data should be loaded or dumped first to update.')
            raise ValueError('.load() or .dump() must be called before update')

        if row_value is None:
            if isinstance(self.loaded_data, list):
                row_value = self.loaded_data[0][identifier]
            else:
                row_value = self.loaded_data[identifier]

        logger.debug(f'Updating with identifier {identifier}'
                     f'with row value {row_value} '
                     f'data for {self.loaded_data} ')

        logger.info('Executing update_data.')
        update_data(self.get_table(), self.loaded_data, identifier, row_value)


    @tracelog(logger)
    def upsert(self, row_value: Any = None, identifier: str = 'id', returning: bool = None, conflict_fields: str = None, chunk_size: int = 200, sql: str = None):
        """Upsert `src_data` in database.

        :params row_value: where clause filter value.
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`.
        :type identifier:str

        :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
        :type conflict_fields: str

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk insert isn't implemented.

        :raises ValueError: Raised when update() is called before loading or dumping data.
        """
        self.check_src_data()
        if self.is_loaded is False and self.is_dumped is False:
            logger.critical('Data should be loaded or dumped first to update.')
            raise ValueError('.load() or .dump() must be called before update')

        if row_value is None:
            if isinstance(self.loaded_data, list):
                row_value = self.loaded_data[0][identifier]
            else:
                row_value = self.loaded_data[identifier]

        logger.info('Executing upsert_data.')
        try:
            logger.debug(f'Saving data: {self.src_data}')
            query_list = upsert_bulk_data(
                self.get_table(),
                self.src_data,
                chunk_size=chunk_size,
                returning=returning,
                conflict_fields=conflict_fields,
                sql=sql
            )

        except Exception as ex:
            logger.info(ex)
            logger.error(ex, exc_info=True)
            logger.error(f'Error during updating data for {self.loaded_data}', exc_info=True)

        return query_list
