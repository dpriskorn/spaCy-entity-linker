"""This script enables enhancing the database with additional languages

Algorithm:
create new columns for the additional language to support
create new table for aliases lang_aliases e.g. sv_aliases
iterate over the latest wikidata dumpfile using qwikidata
for each entry in the dumpfile lookup the QID in the sqlite database
if QID is found:
  add label, description, aliases to the database
"""
import gzip
import logging
import os
import sqlite3
from sqlite3 import OperationalError, Cursor
from typing import Any, Set

from pydantic import BaseModel
from qwikidata.entity import WikidataItem, WikidataProperty
from qwikidata.json_dump import WikidataJsonDump
from qwikidata.typedefs import LanguageCode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhanceDatabaseFromDump(BaseModel):
    """TODO avoid hardcoding sv in the queries"""
    connection: Any = None
    tuple_cursor: Cursor = None
    row_cursor: Cursor = None
    lang: str = "sv"
    file_path: str = '/mnt/nfs/dumps-clouddumps1001.wikimedia.org/wikidatawiki/entities/latest-all.json.gz'
    item_ids_to_process: Set[int] = set()

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.connect_to_db()
        self.initialize_cursors()
        self.alter_db_structure()
        self.create_indexes()
        self.get_item_ids_left_to_process()
        self.iterate_lines()
        # self.extract_lines_using_grep()
        # self.iterate_json_dump()
        # self.commit_and_close_db()

    def iterate_lines(self):
        """We iterate the dump line by line to
        avoid parsing the JSON of ~95 mil unwanted entries"""
        print("Iterating lines")
        if not os.path.exists(self.file_path):
            raise ValueError("dump file path does not exist")
        line_number = 1
        with gzip.open(self.file_path, 'rt') as file:
            for line in file:
                print(f"QIDs left to process: {self.item_ids_to_process}")
                print(f"Working on dump line number: {line_number}")
                # print(line.strip())
                for item_id in self.item_ids_to_process:
                    pattern = f'"type":"item","id":"Q{item_id}"'
                    if pattern in line:
                        print(f"Found match! id: {item_id}")
                exit()
            line_number += 1

    def iterate_json_dump(self) -> None:
        wjd = WikidataJsonDump(self.file_path)

        type_to_entity_class = {"item": WikidataItem, "property": WikidataProperty}
        max_entities = 1
        entities = []

        for ii, entity_dict in enumerate(wjd):
            if ii >= max_entities or not self.item_ids_left_to_process:
                break
            # entity_id = entity_dict["id"]
            entity_type = entity_dict["type"]
            entity = type_to_entity_class[entity_type](entity_dict)
            entities.append(entity)

        for entity in entities:
            entity: WikidataItem
            logger.info(f"qid: {entity.entity_id}")
            logger.info(f"label: "
                        f"{entity.get_label(lang=LanguageCode(self.lang))}")
            logger.info(f"description: "
                        f"{entity.get_description(lang=LanguageCode(self.lang))}")
            logger.info(f"aliases: "
                        f"{entity.get_aliases(lang=LanguageCode(self.lang))}")
            result = self.fetch_data(entity=entity)
            print(result["processed"])
            if result and result["processed"] == 0:
                self.update(entity=entity)
                self.check_table()
                self.verify_result(item_id=self.item_id(entity=entity))
            else:
                if result["processed"] == 1:
                    print("already processed")
                else:
                    print("item_id not in database")
            print()
            # break

    def fetch_data(self, entity: WikidataItem):
        sql_query = "SELECT * FROM joined WHERE item_id = ?"
        params = (self.item_id(entity=entity),)  # A tuple containing the parameter(s)
        self.row_cursor.execute(sql_query, params)
        return self.row_cursor.fetchone()

    def get_item_ids_left_to_process(self) -> None:
        logger.info("Getting item_ids left to process")
        query = """SELECT item_id
        FROM joined
        WHERE processed = 0;
        """
        self.row_cursor.execute(query)
        item_ids = [row["item_id"] for row in self.row_cursor.fetchall()]
        self.item_ids_to_process = set(item_ids)
        logger.info(f"Got {len(item_ids)} item_ids left to process")

    @property
    def item_ids_left_to_process(self) -> bool:
        query = """SELECT COUNT(*) AS count_processed_false
        FROM joined
        WHERE processed = 0;
        """
        self.row_cursor.execute(query)
        count = int(self.row_cursor.fetchone()["count_processed_false"])
        logger.info(f"entities left to process: {count}")
        return bool(count)

    @staticmethod
    def item_id(entity) -> int:
        return int(entity.entity_id[1:])

    def verify_result(self, item_id: int) -> None:
        sql_query = "SELECT * FROM joined WHERE item_id = ?"
        params = (item_id,)  # A tuple containing the parameter(s)
        self.tuple_cursor.execute(sql_query, params)
        result = self.tuple_cursor.fetchall()
        logger.info(result)
        sql_query = "SELECT * FROM sv_aliases WHERE item_id = ?"
        params = (item_id,)  # A tuple containing the parameter(s)
        self.tuple_cursor.execute(sql_query, params)
        result = self.tuple_cursor.fetchall()
        logger.info(result)

    def check_table(self) -> None:
        sql_query = "PRAGMA table_info(joined);"
        self.tuple_cursor.execute(sql_query)
        result = self.tuple_cursor.fetchall()
        print(result)

    def update(self, entity: WikidataItem) -> None:
        self.update_label_and_description(entity=entity)
        self.update_aliases(entity=entity)

    def update_aliases(self, entity: WikidataItem) -> None:
        print("Updating aliases")
        aliases = entity.get_aliases(lang=LanguageCode(self.lang))
        for alias in aliases:
            self.tuple_cursor.execute(
                "UPDATE sv_aliases SET sv_alias = ?, sv_alias_lowercase = ? WHERE item_id = ?",
                (alias, alias.lower(), self.item_id(entity=entity))
            )

    def update_label_and_description(self, entity: WikidataItem) -> None:
        print("Updating label and description")
        label = entity.get_label(lang=LanguageCode(self.lang))
        # print(f"inserting label: {label}")
        desc = entity.get_description(lang=LanguageCode(self.lang))
        # print(f"inserting description: {desc}")
        sql_query = """UPDATE joined
        SET sv_description = ?,
            sv_label = ?,
            processed = True
        WHERE item_id = ?;
        """
        params = (desc, label, self.item_id(entity=entity))  # Using the label twice for both columns
        self.tuple_cursor.execute(sql_query, params)

    def connect_to_db(self) -> None:
        # Replace 'wikidb_filtered.db' with your database file name
        db_file = 'wikidb_filtered.db'
        # Connect to the database
        self.connection = sqlite3.connect(db_file)

    def initialize_cursors(self) -> None:
        # Create cursors to interact with the database
        self.row_cursor = self.connection.cursor()
        self.row_cursor.row_factory = sqlite3.Row
        self.tuple_cursor = self.connection.cursor()
        self.tuple_cursor.row_factory = None

    def create_indexes(self):
        logger.info("Creating indexes")
        query1 = "CREATE INDEX idx_processed ON joined (processed);"
        try:
            self.tuple_cursor.execute(query1)
        except OperationalError:
            # Ignore if they already exist
            pass
        query2 = "CREATE INDEX idx_item_id ON joined (item_id);"
        try:
            self.tuple_cursor.execute(query2)
        except OperationalError:
            # Ignore if they already exist
            pass

    def alter_db_structure(self) -> None:
        query0 = """
        ALTER TABLE joined
        ADD COLUMN processed BOOLEAN DEFAULT FALSE;
        """
        query1 = """
        -- Add columns to the 'joined' table
        ALTER TABLE joined
        ADD COLUMN sv_label TEXT;
        """
        query2 = """
        ALTER TABLE joined
        ADD COLUMN sv_description TEXT;
        """
        query3 = """CREATE TABLE sv_aliases (
            item_id INTEGER PRIMARY KEY,
            sv_alias TEXT,
            sv_alias_lowercase TEXT
        );
        """
        try:
            self.tuple_cursor.execute(query0)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.tuple_cursor.execute(query1)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.tuple_cursor.execute(query2)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.tuple_cursor.execute(query3)
        except OperationalError:
            # Ignore if they already exist
            pass

    def commit_and_close_db(self) -> None:
        # Don't forget to close the connection when done
        # self.conn.commit()
        self.connection.close()


enhancer = EnhanceDatabaseFromDump()
enhancer.start()
