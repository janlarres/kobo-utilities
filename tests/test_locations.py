# ruff: noqa: INP001, PT009
from __future__ import annotations

import dataclasses
import datetime as dt
import os
import sys
import unittest
import uuid
from contextlib import ExitStack
from enum import Enum
from pathlib import Path
from pprint import pprint
from queue import Queue
from typing import TYPE_CHECKING, Any, ClassVar, cast
from unittest import mock

import apsw
from calibre.devices.kobo.books import Book
from calibre.devices.kobo.driver import KOBOTOUCH
from calibre.ebooks.metadata import MetaInformation
from calibre.utils.logging import default_log

test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path = [test_dir, *sys.path]

if TYPE_CHECKING:
    from koboutilities import action, config, utils
    from koboutilities.action import KoboUtilitiesAction
    from koboutilities.constants import BOOK_CONTENTTYPE
    from koboutilities.features import locations
else:
    from calibre_plugins.koboutilities import action, config, utils
    from calibre_plugins.koboutilities.action import KoboUtilitiesAction
    from calibre_plugins.koboutilities.constants import BOOK_CONTENTTYPE
    from calibre_plugins.koboutilities.features import locations

TIMESTAMP_STRING = KOBOTOUCH.TIMESTAMP_STRING


class ReadStatus(Enum):
    UNREAD = 0
    READING = 1
    FINISHED = 2


@dataclasses.dataclass
class TestBook:
    title: str
    authors: list[str]
    rating: int | None
    chapter_id: str | None
    read_status: ReadStatus
    percent_read: int | None
    last_read: dt.datetime | None
    time_spent_reading: int | None
    rest_of_book_estimate: int | None
    is_kepub: bool
    contentID: str = dataclasses.field(init=False)
    mime_type: str = dataclasses.field(init=False)
    calibre_id: int = dataclasses.field(init=False)
    _next_calibre_id: ClassVar[int] = 1

    def __post_init__(self):
        self.contentID = uuid.uuid4().hex
        self.mime_type = (
            "application/x-kobo-epub+zip" if self.is_kepub else "application/epub+zip"
        )
        self.calibre_id = TestBook._next_calibre_id
        TestBook._next_calibre_id += 1

    def to_calibre_book(self) -> Book:
        mi = MetaInformation(self.title, self.authors)
        book = Book("", "lpath", title=mi.title, other=mi)
        book.calibre_id = self.calibre_id
        book.contentIDs = [self.contentID]
        book.rating = self.rating
        book.set_all_user_metadata(
            {
                "#chapter_id": {"datatype": "text", "#value#": self.chapter_id},
                "#percent_read": {"datatype": "int", "#value#": self.percent_read},
                "#last_read": {"datatype": "datetime", "#value#": self.last_read},
                "#time_spent_reading": {
                    "datatype": "int",
                    "#value#": self.time_spent_reading,
                },
                "#rest_of_book_estimate": {
                    "datatype": "int",
                    "#value#": self.rest_of_book_estimate,
                },
            }
        )

        return book


class DeviceDb:
    def __init__(self):
        schema = Path(test_dir, "kobo-schema.sql").read_text()
        self.db_conn = apsw.Connection(":memory:")
        self.db_conn.setrowtrace(row_factory)
        self.cursor = self.db_conn.cursor()
        self.cursor.execute(schema)

        self.sync_time = dt.datetime(
            2002, 9, 9, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=0))
        ).strftime(TIMESTAMP_STRING)

    def insert_books(self, *books: TestBook) -> None:
        for book in books:
            self.cursor.execute(
                """
                    INSERT INTO content (
                        ContentID,
                        ContentType,
                        MimeType,
                        ___SyncTime,
                        ___UserID,
                        Title,
                        ChapterIDBookmarked,
                        ReadStatus,
                        ___PercentRead,
                        DateLastRead,
                        TimeSpentReading,
                        RestOfBookEstimate
                    ) VALUES (
                        :contentID,
                        :content_type,
                        :mime_type,
                        :sync_time,
                        :user_id,
                        :title,
                        :chapter_id,
                        :read_status,
                        :percent_read,
                        :last_read,
                        :time_spent_reading,
                        :rest_of_book_estimate
                    )
                """,
                {
                    **dataclasses.asdict(book),
                    "content_type": BOOK_CONTENTTYPE,
                    "sync_time": self.sync_time,
                    "user_id": "",
                    "read_status": book.read_status.value,
                    "last_read": book.last_read.strftime(TIMESTAMP_STRING)
                    if book.last_read is not None
                    else None,
                },
            )
            self.cursor.execute(
                "INSERT INTO ratings (ContentID, Rating, DateModified) VALUES (?, ?, ?)",
                (
                    book.contentID,
                    book.rating,
                    dt.datetime(
                        2002, 1, 1, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=0))
                    ).strftime(TIMESTAMP_STRING),
                ),
            )

    def query_books(self) -> dict[str, dict[str, Any]]:
        test_query = """
            SELECT content.*, ratings.Rating FROM content
            LEFT OUTER JOIN ratings ON content.ContentID = ratings.ContentID
        """
        return {
            book["ContentID"]: book
            # Inferred type doesn't account for row factory
            for book in cast(
                "list[dict[str, Any]]", self.cursor.execute(test_query).fetchall()
            )
        }


class TestLocations(unittest.TestCase):
    def setUp(self):
        self.plugin = KoboUtilitiesAction(None, None)
        driver = mock.MagicMock()
        driver.fwversion = (4, 41, 23145)
        self.plugin.device = action.KoboDevice(
            driver=driver,
            is_kobotouch=True,
            profile=config.ProfileConfig(),
            backup_config=config.BackupOptionsStoreConfig(),
            device_type="",
            drive_info={},
            uuid="1234",
            version_info=config.KoboVersionInfo("1", "1", "1"),
            supports_series=True,
            supports_series_list=True,
            supports_ratings=True,
            epub_location_like_kepub=False,
            name="test-device",
            path="/",
            db_path="/kobo.sqlite",
            device_db_path="/kobo.sqlite",
            is_db_copied=False,
            timestamp_string=TIMESTAMP_STRING,
        )
        self.plugin.log = default_log  # type: ignore[reportAttributeAccessIssue]
        self.queue = Queue()
        self.maxDiff = None

    def test_store_bookmarks(self):
        book1 = TestBook(
            title="Title 1",
            authors=["Author 1"],
            rating=0,
            chapter_id="chapter1",
            read_status=ReadStatus.READING,
            percent_read=50,
            last_read=dt.datetime(
                2000, 1, 2, 12, 34, 56, tzinfo=dt.timezone(dt.timedelta(hours=0))
            ),
            time_spent_reading=100,
            rest_of_book_estimate=200,
            is_kepub=True,
        )
        book2 = TestBook(
            title="Title 2",
            authors=["Author 2"],
            rating=0,
            chapter_id="chapter2",
            read_status=ReadStatus.READING,
            percent_read=25,
            last_read=dt.datetime(
                2000, 1, 2, 12, 34, 56, tzinfo=dt.timezone(dt.timedelta(hours=0))
            ),
            time_spent_reading=300,
            rest_of_book_estimate=400,
            is_kepub=False,
        )
        books_in_calibre = [
            (
                book1.calibre_id,
                [book1.contentID],
                book1.title,
                book1.authors,
                book1.chapter_id,
                book1.percent_read,
                book1.rating,
                book1.last_read,
                book1.time_spent_reading,
                book1.rest_of_book_estimate,
            ),
            (
                book2.calibre_id,
                [book2.contentID],
                book2.title,
                book2.authors,
                book2.chapter_id,
                book2.percent_read,
                book2.rating,
                book2.last_read,
                book2.time_spent_reading,
                book2.rest_of_book_estimate,
            ),
        ]

        device_db = DeviceDb()
        device_db.insert_books(book1, book2)

        # Update reading progress on the device
        # Don't update DateLastRead for first book to test STORE_IF_MORE_RECENT option
        device_db.cursor.execute(
            """
                UPDATE content
                SET ___PercentRead = 90
                WHERE ContentID = ?
            """,
            (book1.contentID,),
        )
        device_db.cursor.execute(
            """
                UPDATE content
                SET ___PercentRead = 75,
                    DateLastRead = ?,
                    TimeSpentReading = 450,
                    RestOfBookEstimate = 250
                WHERE ContentID = ?
            """,
            (
                dt.datetime(
                    2001, 1, 2, 12, 34, 56, tzinfo=dt.timezone(dt.timedelta(hours=0))
                ).strftime(TIMESTAMP_STRING),
                book2.contentID,
            ),
        )

        bookmark_options = config.BookmarkOptionsConfig()
        bookmark_options.clearIfUnread = False
        bookmark_options.storeIfMoreRecent = True
        bookmark_options.doNotStoreIfReopened = True
        custom_columns = config.CustomColumns(
            None,
            "#percent_read",
            None,
            "#last_read",
            "#time_spent_reading",
            "#rest_of_book_estimate",
        )
        options = locations.ReadLocationsJobOptions(
            bookmark_options,
            False,
            locations.FetchQueries(
                locations.KEPUB_FETCH_QUERY, locations.EPUB_FETCH_QUERY
            ),
            "unused",
            "unused",
            False,
            "unused",
            custom_columns,
            supports_ratings=True,
            allOnDevice=True,
            prompt_to_store=False,
        )

        # Run tested function
        with mock.patch.object(
            locations,
            "DeviceDatabaseConnection",
            return_value=device_db.db_conn,
        ):
            stored_locations = locations._read_locations(books_in_calibre, options)

        pprint(stored_locations)
        self.assertNotIn(book1.calibre_id, stored_locations)
        self.assertEqual(stored_locations[book2.calibre_id]["___PercentRead"], 75)
        self.assertEqual(stored_locations[book2.calibre_id]["TimeSpentReading"], 450)
        self.assertEqual(stored_locations[book2.calibre_id]["RestOfBookEstimate"], 250)

    def test_restore_current_bookmark(self):
        book1 = TestBook(
            title="Title A",
            authors=["Author A"],
            rating=3,
            chapter_id=None,
            read_status=ReadStatus.UNREAD,
            percent_read=0,
            last_read=None,
            time_spent_reading=0,
            rest_of_book_estimate=0,
            is_kepub=True,
        )
        book2 = TestBook(
            title="Title B",
            authors=["Author B"],
            rating=5,
            chapter_id="chapter3",
            read_status=ReadStatus.READING,
            percent_read=10,
            last_read=dt.datetime(
                2001, 1, 1, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=0))
            ),
            time_spent_reading=100,
            rest_of_book_estimate=200,
            is_kepub=True,
        )
        book3 = TestBook(
            title="Title C",
            authors=["Author C"],
            rating=0,
            chapter_id="chapter1",
            read_status=ReadStatus.UNREAD,
            percent_read=0,
            last_read=None,
            time_spent_reading=100,
            rest_of_book_estimate=200,
            is_kepub=False,
        )
        books = [book1, book2, book3]

        column_names = config.CustomColumns(
            "#chapter_id",
            "#percent_read",
            "rating",
            "#last_read",
            "#time_spent_reading",
            "#rest_of_book_estimate",
        )

        device_db = DeviceDb()
        device_db.insert_books(*books)
        device_books_before = device_db.query_books()

        # Simulate changing the books in Calibre
        book1.rating = 4
        book1.chapter_id = "chapter2"
        book1.read_status = ReadStatus.READING
        book1.percent_read = 20
        book1.last_read = dt.datetime(
            2001, 1, 15, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=0))
        )
        book1.time_spent_reading = 111
        book1.rest_of_book_estimate = 333
        book2.rating = 2
        book2.chapter_id = "chapter5"
        book2.read_status = ReadStatus.FINISHED
        book2.percent_read = 100
        book2.last_read = dt.datetime(
            2001, 2, 27, 12, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=0))
        )
        book2.time_spent_reading = 555
        book2.rest_of_book_estimate = 0
        book3.time_spent_reading = 0
        book3.rest_of_book_estimate = None

        # Update expected DB values
        db_book1 = device_books_before[book1.contentID]
        db_book1["ChapterIDBookmarked"] = book1.chapter_id
        db_book1["ReadStatus"] = book1.read_status.value
        db_book1["___PercentRead"] = book1.percent_read
        db_book1["DateLastRead"] = book1.last_read.strftime(TIMESTAMP_STRING)
        db_book1["TimeSpentReading"] = book1.time_spent_reading
        db_book1["RestOfBookEstimate"] = book1.rest_of_book_estimate
        db_book1["___SyncTime"] = book1.last_read.strftime(TIMESTAMP_STRING)
        db_book1["FirstTimeReading"] = "false"
        db_book1["Rating"] = int(book1.rating / 2)
        db_book2 = device_books_before[book2.contentID]
        db_book2["ChapterIDBookmarked"] = book2.chapter_id
        db_book2["ReadStatus"] = book2.read_status.value
        db_book2["___PercentRead"] = book2.percent_read
        db_book2["DateLastRead"] = book2.last_read.strftime(TIMESTAMP_STRING)
        db_book2["TimeSpentReading"] = book2.time_spent_reading
        db_book2["RestOfBookEstimate"] = book2.rest_of_book_estimate
        db_book2["___SyncTime"] = book2.last_read.strftime(TIMESTAMP_STRING)
        db_book2["Rating"] = int(book2.rating / 2)
        db_book2["FirstTimeReading"] = "false"
        db_book3 = device_books_before[book3.contentID]
        db_book3["ChapterIDBookmarked"] = None
        db_book3["DateLastRead"] = None
        db_book3["TimeSpentReading"] = 0
        db_book3["RestOfBookEstimate"] = 0
        db_book3["Rating"] = None

        plugin = self.plugin
        options = config.BookmarkOptionsConfig()
        options.readingStatus = True
        options.setDateToNow = False
        options.rating = True

        with ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(config, "get_column_names", return_value=column_names)
            )
            stack.enter_context(
                mock.patch.object(
                    utils, "device_database_connection", return_value=device_db.db_conn
                )
            )
            device = plugin.device
            assert device is not None
            locations._restore_current_bookmark(
                [book.to_calibre_book() for book in books],
                device,
                plugin.gui,
                options,
                None,
            )

        db_books_after = device_db.query_books()
        self.assertDictEqual(device_books_before, db_books_after)


def row_factory(cursor: apsw.Cursor, row: apsw.SQLiteValues):
    return {k[0]: row[i] for i, k in enumerate(cursor.getdescription())}


if __name__ == "__main__":
    unittest.main(module=Path(__file__).stem, verbosity=2)
