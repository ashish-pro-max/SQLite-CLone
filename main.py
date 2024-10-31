import sys
from dataclasses import dataclass
import sqlparse
from .record_parser import parse_record
from .varint_parser import parse_varint
database_file_path = sys.argv[1]
command = sys.argv[2]
class SqliteFileParser:
    def __init__(self, database_file_path):
        self.database_file = open(database_file_path, "rb")
        self.database_file.seek(16)
        self.page_size = int.from_bytes(self.database_file.read(2), "big")
        self.database_file.seek(28)
        self.page_num = int.from_bytes(self.database_file.read(4), "big")
        self.page_headers = self.read_pages()
        self.sqlite_schema_rows = self.get_sqlite_schema_rows(
            self.database_file, self.get_cell_pointers(self.page_headers[0])
        )
    def get_cell_pointers(self, page_header):
        additional_offset = 4 if page_header.page_type in (2, 5) else 0
        self.database_file.seek(page_header.offset + 8 + additional_offset)
        return [
            int.from_bytes(self.database_file.read(2), "big")
            for _ in range(page_header.number_of_cells)
        ]
    def read_pages(self):
        all_pages = []
        self.database_file.seek(100)
        curr_offset = 100
        for i in range(self.page_num):
            all_pages.append(PageHeader.parse_from(self.database_file, curr_offset))
            curr_offset = self.database_file.seek(self.page_size * (i + 1))
        return all_pages
    def get_sqlite_schema_rows(self, database_file, cell_pointers):
        sqlite_schema_rows = {}
        for cell_pointer in cell_pointers:
            database_file.seek(cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            record = parse_record(database_file, 5)
            sqlite_schema_rows[record[2].decode()] = {
                "type": record[0].decode(),
                "name": record[1].decode(),
                "tbl_name": record[2].decode(),
                "rootpage": record[3],
                "sql": record[4].decode(),
            }
        return sqlite_schema_rows
    def get_row_count(self, table):
        table_rootpage = self.sqlite_schema_rows[table]["rootpage"]
        table_page = self.page_headers[table_rootpage - 1]
        print(table_page.number_of_cells)
    def get_sql_info(self, sql_statement):
        columns = None
        table = None
        where = []
        sql_tokens = sqlparse.parse(sql_statement)[0].tokens
        for token in sql_tokens:
            if isinstance(token, sqlparse.sql.IdentifierList) or isinstance(
                token, sqlparse.sql.Function
            ):
                columns = str(token)
            if isinstance(token, sqlparse.sql.Identifier):
                if columns is None:
                    columns = str(token)
                else:
                    table = str(token)
            if isinstance(token, sqlparse.sql.Where):
                for where_token in token.tokens:
                    if isinstance(where_token, sqlparse.sql.Comparison):
                        for comp_token in where_token.tokens:
                            if str(comp_token) != " ":
                                where.append(str(comp_token))
        return {"select": columns, "table": table, "where": where if where else None}
    def get_column_count(self, table):
        create_sql = sqlparse.parse(self.sqlite_schema_rows[table]["sql"])
        columns = create_sql[0][-1].tokens
        total_columns = []
        for token in columns:
            if isinstance(token, sqlparse.sql.Identifier):
                total_columns.append(str(token))
            if isinstance(token, sqlparse.sql.IdentifierList):
                for sub_token in token:
                    if (
                        isinstance(sub_token, sqlparse.sql.Identifier)
                        and str(sub_token) != "autoincrement"
                    ):
                        total_columns.append(str(sub_token))
        return total_columns
    def get_records(self, table_page, columns):
        cell_pointers = self.get_cell_pointers(table_page)
        records = []
        column_count = len(columns)
        for pointer in cell_pointers:
            self.database_file.seek(pointer + table_page.offset)
            if table_page.page_type == 13:
                total_bytes = parse_varint(self.database_file)
                row_id = parse_varint(self.database_file)
                record = parse_record(self.database_file, column_count)
                record[0] = row_id
                record = [c.decode() if isinstance(c, bytes) else c for c in record]
                record = {columns[i]: record[i] for i in range(len(columns))}
                records.append(record)
            elif table_page.page_type == 5:
                left_child_pointer = int.from_bytes(self.database_file.read(4), "big")
                int_key = parse_varint(self.database_file)
                records += self.get_records(
                    self.page_headers[left_child_pointer - 1], columns
                )
            else:
                print("NEW PAGE TYPE: " + table_page.page_type)
        return records
    def execute_sql(self, sql_statement):
        sql_info = self.get_sql_info(sql_statement)
        table = sql_info["table"]
        if sql_info["select"].upper() == "COUNT(*)":
            self.get_row_count(table)
            return
        columns = self.get_column_count(table)
        column_count = len(columns)
        table_rootpage = self.sqlite_schema_rows[table]["rootpage"]
        table_page = self.page_headers[table_rootpage - 1]
        self.database_file.seek(table_page.offset + 8)
        
        records = self.get_records(table_page, columns)
        columns_of_interest = sql_info["select"].split(", ")
        for i, record in enumerate(records):
            total_row = []
            if sql_info["where"] is None:
                for col in columns_of_interest:
                    total_row.append(str(record[col]))
            else:
                comparison = sql_info["where"]
                comp_col = comparison[0]
                comp_val = comparison[2].replace("'", "")
                if record[comp_col] == comp_val:
                    for col in columns_of_interest:
                        total_row.append(str(record[col]))
            if len(total_row) > 0:
                print("|".join(total_row))
@dataclass(init=False)
class PageHeader:
    page_type: int
    first_free_block_start: int
    number_of_cells: int
    start_of_content_area: int
    fragmented_free_bytes: int
    offset: int
    @classmethod
    def parse_from(cls, database_file, offset):
        instance = cls()
        instance.offset = offset
        instance.page_type = int.from_bytes(database_file.read(1), "big")
        instance.first_free_block_start = int.from_bytes(database_file.read(2), "big")
        instance.number_of_cells = int.from_bytes(database_file.read(2), "big")
        instance.start_of_content_area = int.from_bytes(database_file.read(2), "big")
        instance.fragmented_free_bytes = int.from_bytes(database_file.read(1), "big")
        return instance
sqllite_file_parser = SqliteFileParser(database_file_path)
if command == ".dbinfo":
    print("Logs from your program will appear here!")
    print(f"number of tables: {len(sqllite_file_parser.sqlite_schema_rows)}")
elif command == ".tables":
    print(" ".join(sqllite_file_parser.sqlite_schema_rows.keys()))
else:
    sqllite_file_parser.execute_sql(command)