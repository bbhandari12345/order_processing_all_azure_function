from op_extractor.utils.data_access_layer.sql_db import DBEngineFactory
from op_extractor.constants import DB_CONFIG
import logging


# TODO for ops monitoring pg_stat statement
li_db = DBEngineFactory.get_db_engine(DB_CONFIG, DBEngineFactory.POSTGRES)


def get_logger():
    return logging
