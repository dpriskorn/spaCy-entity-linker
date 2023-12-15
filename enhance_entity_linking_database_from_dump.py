"""This script enables enhancing the database with additional languages

Algorithm:
create new columns for the additional language to support
create new table for aliases lang_aliases e.g. sv_aliases
iterate over the latest wikidata dumpfile using qwikidata
for each entry in the dumpfile lookup the QID in the sqlite database
if QID is found:
  add label, description, aliases to the database
"""
import logging
from sqlite3 import OperationalError
from typing import Any

from pydantic import BaseModel
from qwikidata.json_dump import WikidataJsonDump
from qwikidata.typedefs import LanguageCode
from qwikidata.entity import WikidataItem, WikidataProperty
import sqlite3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhanceDatabaseFromDump(BaseModel):
    """TODO avoid hardcoding sv in the queries"""
    conn: Any = None
    cursor: Any = None
    lang: str = "sv"

    def iterate_json_dump(self):
        wjd = WikidataJsonDump('/mnt/nfs/dumps-clouddumps1001.wikimedia.org'
                               '/wikidatawiki/entities/latest-all.json.gz')

        type_to_entity_class = {"item": WikidataItem, "property": WikidataProperty}
        max_entities = 1
        entities = []

        for ii, entity_dict in enumerate(wjd):
            if ii >= max_entities or self.all_has_been_processed:
                break
            # entity_id = entity_dict["id"]
            entity_type = entity_dict["type"]
            entity = type_to_entity_class[entity_type](entity_dict)
            entities.append(entity)

        self.connect_to_db()
        self.get_cursor()
        self.alter_db_structure()

        for entity in entities:
            entity: WikidataItem
            logger.info(f"qid: {entity.entity_id}")
            logger.info(f"label: {entity.get_label(lang=LanguageCode(self.lang))}")
            logger.info(f"description: {entity.get_description(lang=LanguageCode(self.lang))}")
            logger.info(f"aliases: {entity.get_aliases(lang=LanguageCode(self.lang))}")
            sql_query = "SELECT * FROM joined WHERE item_id = ?"
            params = (self.item_id(entity=entity),)  # A tuple containing the parameter(s)
            self.cursor.execute(sql_query, params)
            result = self.cursor.fetchall()
            print(result)
            if result:
                self.update(entity=entity)
                self.check_table()
                self.verify_result(item_id=self.item_id(entity=entity))
            print()
            # break
        self.commit_and_close_db()

    @property
    def all_has_been_processed(self) -> bool:
        query = """SELECT COUNT(*) AS count_processed_false
        FROM joined
        WHERE processed = 'false';
        """
        self.cursor.execute(query)
        count = int(self.cursor.fetchall())
        logger.info(f"entities left to process: {count}")
        return bool(count)

    def item_id(self, entity) -> int:
        return int(entity.entity_id[1:])

    def verify_result(self, item_id: int):
        sql_query = "SELECT * FROM joined WHERE item_id = ?"
        params = (item_id,)  # A tuple containing the parameter(s)
        self.cursor.execute(sql_query, params)
        result = self.cursor.fetchall()
        logger.info(result)
        sql_query = "SELECT * FROM sv_aliases WHERE item_id = ?"
        params = (item_id,)  # A tuple containing the parameter(s)
        self.cursor.execute(sql_query, params)
        result = self.cursor.fetchall()
        logger.info(result)

    def check_table(self):
        sql_query = "PRAGMA table_info(joined);"
        self.cursor.execute(sql_query)
        result = self.cursor.fetchall()
        print(result)

    def update(self, entity: WikidataItem):
        self.update_label_and_description(entity=entity)
        self.update_aliases(entity=entity)

    def update_aliases(self, entity: WikidataItem):
        print("Updating aliases")
        aliases = entity.get_aliases(lang=LanguageCode(self.lang))
        for alias in aliases:
            self.cursor.execute(
                "UPDATE sv_aliases SET sv_alias = ?, sv_alias_lowercase = ? WHERE item_id = ?",
                (alias, alias.lower(), self.item_id(entity=entity))
            )

    def update_label_and_description(self, entity: WikidataItem):
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
        self.cursor.execute(sql_query, params)

    def connect_to_db(self):

        # Replace 'wikidb_filtered.db' with your database file name
        db_file = 'wikidb_filtered.db'

        # Connect to the database
        self.conn = sqlite3.connect(db_file)

    def get_cursor(self):
        # Create a cursor object to interact with the database
        self.cursor = self.conn.cursor()

    def alter_db_structure(self):
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
            self.cursor.execute(query0)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.cursor.execute(query1)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.cursor.execute(query2)
        except OperationalError:
            # Ignore if they already exist
            pass
        try:
            self.cursor.execute(query3)
        except OperationalError:
            # Ignore if they already exist
            pass

    def commit_and_close_db(self):
        # Don't forget to close the connection when done
        # self.conn.commit()
        self.conn.close()

enhancer = EnhanceDatabaseFromDump()
enhancer.iterate_json_dump()
