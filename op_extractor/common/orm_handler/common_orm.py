""" Collections of most common db queries"""
from typing import Any, Union, Optional, Sequence
from op_extractor.utils.logger import tracelog
from op_extractor.conf import li_db, get_logger

logger = get_logger()


@tracelog(logger)
def insert_data(table: str, inserted_data: dict, include=None, returning: bool = True) -> dict:
    """Save given single data to li_db table.

    Prepares insert Query with placeholders from insert_data dict keys
    and inserts in a single transaction.

    :params table: table to insert into.
    :type table: str

    :params insert_data: new record value in dict where keys must map to table columns.
    :type inserted_data: dict

    :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
    :type include: list

    :params returning: Prepare query to return after insert if true else no return insert.
    :type returning: bool

    :returns: Inserted data points back from database in a dict.
    :rtype: dict

    :raises Exception: Raised when error occurs in insert transaction.
    """
    sql = generate_insert_sql(table, inserted_data, include, returning)
    values = list(inserted_data.values())

    try:
        with li_db.transaction(auto_commit=True) as query_set:
            result_set = query_set.execute_query(sql, values)
            logger.debug(f'Executed Non Query {query_set.query}')
            return result_set.to_list()
    except Exception as e:
        logger.error('Error in insert_data execution.', exc_info=True)
        raise e


@tracelog(logger)
def update_data(table: str, updated_data: dict, row_identifier: str, row_value: Any) -> None:
    """Update table data.

    Prepares update query with placeholders from update_data dict keys and
    updates in a single transaction.

    :params table: table to update.
    :type table: str

    :params update_data: updated record value in dict where keys must map to table columns.
    :type updated_data: dict

    :params row_value: where clause filter value.
    :type row_value: Any

    :params row_identifier:where clause filter column name.
    :type row_identifier: str

    :raises Exception: Raised when error occurs in update transaction.
    """
    sql = generate_update_sql(table, updated_data, row_identifier)
    # Using dictionary as placeholders instead of tuples
    # Preparing place golder data along with where clause.
    # Specific to python 3.9 `|` mergers two dicts
    updated_data = updated_data | {row_identifier: row_value}
    try:
        with li_db.transaction(auto_commit=True) as query_set:
            query_set.execute_non_query(sql, updated_data)
            logger.debug(f'Executed non Query {query_set.query}')
    except Exception as e:
        logger.error('Error in update_data execution.', exc_info=True)
        raise e


def generate_insert_sql(table: str, inserted_data: dict, include, returning) -> str:
    """Prepare insert query with placeholders from insert_data dict keys.

        eg::
            INSERT INTO table_name (col1,col2) VALUES ();

        :params table: table to insert into.
        :type table: str

        :params insert_data: new record value in dict where keys must map to table columns.
        :type inserted_data: dict

        :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
        :type include: list

        :params returning: Prepare query to return after insert if true else no return insert.
        :type returning: bool

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data insert.')
    placeholders = ', '.join(['%s'] * len(inserted_data))
    columns = ', '.join(inserted_data.keys())

    if returning:
        logger.debug(
            'Returning param is set to True. Preparing INSERT with returning.')
        if include:
            returning_cols = ', '.join(
                (set(include + list(inserted_data.keys()))))
        else:
            returning_cols = columns

        sql = ('INSERT INTO %s ( %s ) VALUES ( %s ) '
               'RETURNING %s;' % (table, columns, placeholders, returning_cols))  # noqa: S608
    else:
        logger.debug(
            'Returning param is set to False. Preparing INSERT without returning.')
        sql = ('INSERT INTO %s ( %s ) VALUES ( %s )' % (table, columns, placeholders))  # noqa: S608

    logger.debug(f'Generated SQL statement {sql}')
    return sql


def generate_update_sql(table: str, updated_data: dict, row_identifier: str) -> str:
    """Prepare update query with placeholders from update_data dict keys.

        eg::
           `UPDATE <table> SET key1=%(key1)s, key2=%(key2)s WHERE <row_identifier> =%(row_identifier)s;`

        :params table: table to update.
        :type table: str

        :params update_data: updated record value in dict where keys must map to table columns.
        :type updated_data: dict

        :params row_identifier:where clause filter column name.
        :type row_identifier: str

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data update.')
    set_placeholders = ', '.join(
        [f'{key}=%({key})s' for key in updated_data.keys()])
    where_clause = f' {row_identifier} =%({row_identifier})s'
    sql = 'UPDATE %s SET %s WHERE%s' % (table, set_placeholders, where_clause)  # noqa: S608
    logger.debug(f'Generated SQL statement {sql}')
    return sql


def generate_bulk_upsert_sql(table: str, insert_data: list, include, returning, conflict_fields: str = None, sql: str = None) -> str:
    """Prepare insert query with placeholders from insert_data dict keys.

        eg::
            INSERT INTO table_name (col1,col2) VALUES ();

        :params table: table to insert into.
        :type table: str

        :params insert_data: new record value in dict where keys must map to table columns.
        :type insert_data: dict

        :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
        :type include: list

        :params returning: Preapre query to return after insert if true else no return insert.
        :type returning: bool

        :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
        :type conflict_fields: str

        :returns: Generated SQL Query string and columns.
        :rtype: Tuple[str, str]
        """
    logger.info('Generating SQL statement for data upsert.')
    placeholders = ', '.join(['%s'] * len(insert_data))
    columns = ', '.join(insert_data[0].keys())

    try:
        if returning is True and not sql:
            logger.debug('Returning param is set to True. Preparing INSERT with on conflict set to update.')
            if include is None:
                returnset = set(list(insert_data[0].keys()))
                pre_res = [f'{keys} = EXCLUDED.' + keys for keys in returnset if keys not in conflict_fields.split(',')]
                returning_cols = ', '.join(set(pre_res))
            else:
                returning_cols = columns

            sql = """INSERT INTO %s ( %s ) VALUES  %s ON CONFLICT (%s) DO UPDATE SET %s;""" % (
                table, columns, placeholders, conflict_fields, returning_cols)

        elif returning is False and not sql:
            logger.debug('Returning param is set to False. Upsert with on conflict set to do nothing.')

            sql = """INSERT INTO %s ( %s ) VALUES  %s ON CONFLICT (%s) DO NOTHING;""" % (
                table, columns, placeholders, conflict_fields)

        elif sql:
            logger.debug('Returning param is set to None. Preparing INSERT without returning.')
            sql = sql % (table, columns, placeholders)

        else:
            logger.debug('Returning param is set to None. Preparing INSERT without returning.')
            sql = """INSERT INTO %s ( %s ) VALUES %s;""" % (table, columns, placeholders)

    except Exception as e:
        logger.error("Couldnot create statement for bulk upsert", exc_info=True)
        logger.error(e, exc_info=True)

    logger.info(f'Generated SQL statement')
    return sql


def execute_sql_query(query: str, data: Optional[Sequence] = None):
    """
    :param query:
    :return:
    """
    try:
        with li_db.transaction(auto_commit=True) as query_set:
            queryset = query_set.execute_query(query, data)
            logger.debug(f'Executed Query {query_set.query}')
    except Exception as e:
        logger.error(f'Error while executing {query_set.query}', exc_info=True)
        raise e

    return queryset


@tracelog(logger)
def upsert_bulk_data(table: str, insert_data: list, chunk_size: int, conflict_fields: str = None, include=None, returning: bool = True, sql: str = None) -> Union[list, None]:
    """Save given multiple data to li_db table.

    Prepares insert Query with placeholders from insert_data keys
    and inserts in a single transaction.

    :params table: table to insert into.
    :type table: str

    :params insert_data: new record value in dict where keys must map to table columns.
    :type inserted_data: dict

    :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
    :type include: list

    :params returning: Prepare query to return after insert if true else no return insert.
    :type returning: bool

    :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
    :type conflict_fields: str

    :returns: Inserted data points back from database in a list.
    :rtype: list

    :raises Exception: Raised when error occurs in insert transaction.
    """

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    split_body = chunks(insert_data, chunk_size)

    if sql:
        query_set_list = []

    for items in split_body:
        sql_query = generate_bulk_upsert_sql(
            table, items, include, returning, conflict_fields, sql=sql
        )
        value_list = []
        for data in items:
            values = list(data.values())
            value_list.append(values)

        try:
            with li_db.transaction(auto_commit=False) as query_set:
                tuples = tuple(tuple(x) for x in value_list)
                resultset = query_set.execute_query(sql_query, tuples)
                if sql:
                    query_set_list.extend(resultset.to_list()[0].get('jsonb_agg'))
                logger.info(f'Executed Query')
                li_db.commit()
        except Exception as e:
            logger.info(e)
            logger.error('Error in upsert_data execution.', exc_info=True)
            raise e

    return query_set_list if sql else None
