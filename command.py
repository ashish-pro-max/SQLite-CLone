from typing import BinaryIO, List
import sqlparse
from sqlparse.sql import Token, Function, Identifier, IdentifierList
from sqlparse.tokens import Wildcard
from app.db_manager import DbManager
from app.model.table import Table
def process_sql(database_file: BinaryIO, sqlstr: str) -> None:
    sqlstr = sqlstr.lower()
    tokens: List[Token] = sqlparse.parse(sqlstr)[0].tokens
    table_identifer = tokens[-1]
    assert isinstance(
        table_identifer, Identifier
    ), "last sql token should be table identifier"
    selected = tokens[2]  
    manager = DbManager.from_file(database_file)
    table: Table = manager.get_table(table_identifer.get_real_name())
    if isinstance(selected, Function):
        print(table.num_rows())
    elif isinstance(selected, Identifier):
        col_name = selected.get_real_name()
        col_index = table.get_col_index(col_name)
        for record in table.record_iter():
            print(record.get_col_value_str(col_index))
    elif isinstance(selected, IdentifierList):
        col_identifiers = list(selected.get_identifiers())
        col_names = [col_id.get_real_name() for col_id in col_identifiers]
        col_indices = [table.get_col_index(colname) for colname in col_names]
        for record in table.record_iter():
            print(
                "|".join(
                    record.get_col_value_str(col_index) for col_index in col_indices
                )
            )
    elif isinstance(selected, Wildcard):
        raise NotImplementedError
    else:
        raise ValueError(f"Unsupported {sqlstr}")