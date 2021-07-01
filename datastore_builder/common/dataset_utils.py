import logging
import json
from pathlib import Path
from typing import List, Tuple, Union
#import datetime
#import csv
import sqlite3 as db

# See https://pypi.org/project/jsonschema/
# pip install jsonschema==3.2.0
from jsonschema import validate
from jsonschema.exceptions import ValidationError


class DatasetUtils():
    # @staticmethod
    # def search_dictionary(var:Union[dict, list], search_key:str):
    #     """Takes a dict with nested lists and dicts,
    #     and searches all dicts (recursively) for the spesified search_key.
    #     """
    #     if isinstance(var, dict):
    #         for k, v in var.items():
    #             if k == search_key:
    #                 yield v
    #             if isinstance(v, (dict, list)):
    #                 yield from search_dictionary(v, search_key)
    #     elif isinstance(var, list):
    #         for d in var:
    #             yield from search_dictionary(d, search_key)


    @staticmethod
    def read_json_file(json_file: Path) -> Union[dict, None]:
        with open(json_file, encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                logging.error("Not a valid JSON file: " + str(json_file) \
                    + "\n" + str(e)
                )
                return None


    @staticmethod
    def write_json_file(dict_obj: dict, json_file: Path):
        json_file.write_text(json.dumps(dict_obj, indent=4, ensure_ascii=False), encoding="utf-8")


    @staticmethod
    def is_json_file_valid(json_file: Path, json_schema_file: Path) -> Tuple[bool, str]:
        """Validate the metadata file (JSON) using JSON Schema."""
        with open(json_schema_file, mode="r", encoding="utf-8") as json_schema_dataset_file: 
            json_schema_dataset = json.load(json_schema_dataset_file)

        with open(json_file, mode="r", encoding="utf-8") as dataset_metadata_json_file:
            dataset_metadata_json = json.load(dataset_metadata_json_file)

        try:
            # If no exception is raised by validate(), the instance is valid.
            validate(
                instance=dataset_metadata_json, 
                schema=json_schema_dataset
            )
            return (True, "OK")
        except ValidationError as err:
            return (False, "ERROR in metadata JSON-file:\n" + str(err))


    @staticmethod
    def create_temp_sqlite_db_file(db_file: Path) -> Tuple[db.Connection, db.Cursor]:
        if db_file.exists():
            db_file.unlink()  # removes a file or symbolic link

        sql_create_table = f"""\
            CREATE TABLE temp_dataset (
                unit_id TEXT NOT NULL, 
                value TEXT NOT NULL, 
                start TEXT, 
                stop TEXT, 
                attributes TEXT) """

        db_conn = db.connect(db_file) 
        cursor = db_conn.cursor()
        cursor.execute(sql_create_table)
        # Set Sqlite-params to speed up performance
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("BEGIN TRANSACTION")
        return (db_conn, cursor)


    @staticmethod
    def read_temp_sqlite_db_data_sorted(db_file: Path) -> Tuple[db.Connection, db.Cursor]:
        sql_select_sorted = f"""\
            SELECT unit_id, value, start, stop, attributes
            FROM temp_dataset
            ORDER BY unit_id, start, stop """

        db_conn = db.connect(db_file) 
        cursor = db_conn.cursor()
        #cursor.execute("PRAGMA synchronous = OFF")
        #cursor.execute("BEGIN TRANSACTION")
        cursor.execute(sql_select_sorted)
        return (db_conn, cursor)