from __future__ import annotations

import pickle
import time
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, cast

from calibre import strftime
from calibre.devices.kobo.books import Book
from calibre.ebooks.metadata import authors_to_string
from calibre.ebooks.metadata.book.base import Metadata
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.library.delegates import DateDelegate
from calibre.utils.ipc.job import ParallelJob
from calibre.utils.ipc.server import Server
from qt.core import (
    QAbstractItemView,
    QCheckBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QLabel,
    QProgressDialog,
    QRadioButton,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QTimer,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME, MIMETYPE_KOBO
from ..dialogs import (
    AuthorsTableWidgetItem,
    CheckableTableWidgetItem,
    DateTableWidgetItem,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    RatingTableWidgetItem,
)
from ..utils import DeviceDatabaseConnection, debug

if TYPE_CHECKING:
    import datetime as dt

    from calibre.db.legacy import LibraryDatabase
    from calibre.gui2 import ui
    from calibre.gui2.device import DeviceJob

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


BOOKMARK_SEPARATOR = (
    "|@ @|"  # Spaces are included to allow wrapping in the details panel
)


@dataclass
class FetchQueries:
    kepub: str
    epub: str


@dataclass
class ReadLocationsJobOptions:
    bookmark_options: cfg.BookmarkOptionsConfig
    epub_location_like_kepub: bool
    fetch_queries: FetchQueries
    database_path: str
    device_database_path: str
    is_db_copied: bool
    profile_name: str | None
    custom_columns: cfg.CustomColumns | None
    supports_ratings: bool
    allOnDevice: bool
    prompt_to_store: bool


EPUB_FETCH_QUERY = (
    "SELECT c1.ChapterIDBookmarked, "
    "c2.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "r.rating, "
    "c1.contentId, "
    "c1.TimeSpentReading, "
    "c1.RestOfBookEstimate "
    "FROM content c1 LEFT OUTER JOIN content c2 ON c1.ChapterIDBookmarked = c2.ContentID "
    "LEFT OUTER JOIN ratings r ON c1.ContentID = r.ContentID "
    "WHERE c1.ContentID = ?"
)

EPUB_FETCH_QUERY_NORATING = (
    "SELECT c1.ChapterIDBookmarked, "
    "c2.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "NULL as rating, "
    "c1.contentId, "
    "FROM content c1 LEFT OUTER JOIN content c2 ON c1.ChapterIDBookmarked = c2.ContentID "
    "WHERE c1.ContentID = ?"
)

EPUB_FETCH_QUERY_NOTIMESPENT = (
    "SELECT c1.ChapterIDBookmarked, "
    "c2.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "r.rating, "
    "c1.contentId "
    "FROM content c1 LEFT OUTER JOIN content c2 ON c1.ChapterIDBookmarked = c2.ContentID "
    "LEFT OUTER JOIN ratings r ON c1.ContentID = r.ContentID "
    "WHERE c1.ContentID = ?"
)

KEPUB_FETCH_QUERY = (
    "SELECT c1.ChapterIDBookmarked, "
    "c1.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "r.rating, "
    "c1.contentId, "
    "c1.TimeSpentReading, "
    "c1.RestOfBookEstimate "
    "FROM content c1 LEFT OUTER JOIN ratings r ON c1.ContentID = r.ContentID "
    "WHERE c1.ContentID = ?"
)

KEPUB_FETCH_QUERY_NORATING = (
    "SELECT c1.ChapterIDBookmarked, "
    "c1.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "NULL as rating, "
    "c1.contentId, "
    "FROM content c1 "
    "WHERE c1.ContentID = ?"
)

KEPUB_FETCH_QUERY_NOTIMESPENT = (
    "SELECT c1.ChapterIDBookmarked, "
    "c1.adobe_location, "
    "c1.ReadStatus, "
    "c1.___PercentRead, "
    "c1.Attribution, "
    "c1.DateLastRead, "
    "c1.Title, "
    "c1.MimeType, "
    "r.rating, "
    "c1.contentId "
    "FROM content c1 LEFT OUTER JOIN ratings r ON c1.ContentID = r.ContentID "
    "WHERE c1.ContentID = ?"
)

# Dictionary of Reading status fetch queries
# Key is earliest firmware version that supports this query.
FETCH_QUERIES: dict[tuple[int, int, int], FetchQueries] = {}
FETCH_QUERIES[(0, 0, 0)] = FetchQueries(
    KEPUB_FETCH_QUERY_NORATING, EPUB_FETCH_QUERY_NORATING
)
FETCH_QUERIES[(1, 9, 17)] = FetchQueries(
    KEPUB_FETCH_QUERY_NOTIMESPENT, EPUB_FETCH_QUERY_NOTIMESPENT
)
FETCH_QUERIES[(4, 0, 7523)] = FetchQueries(KEPUB_FETCH_QUERY, EPUB_FETCH_QUERY)
# With 4.17.13651, epub location is stored in the same way a for kepubs.
FETCH_QUERIES[(4, 17, 13651)] = FetchQueries(KEPUB_FETCH_QUERY, KEPUB_FETCH_QUERY)

load_translations()


def handle_bookmarks(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    selectedIDs = utils.get_selected_ids(gui)

    if len(selectedIDs) == 0:
        return

    dlg = BookmarkOptionsDialog(gui, device)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return
    profile_name = dlg.profile_name
    # We know that this cannot be none if the dialog succeeded
    assert profile_name is not None

    if cfg.plugin_prefs.BookmarkOptions.storeBookmarks:
        store_current_bookmark(device, gui, dispatcher, profile_name)
    else:
        restore_current_bookmark(device, gui, profile_name)


def store_current_bookmark(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    profile_name: str,
) -> None:
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    fetch_queries = _get_fetch_query_for_firmware_version(
        cast("tuple[int, int, int]", device.driver.fwversion)
    )
    if fetch_queries is None:
        error_dialog(
            gui,
            _("Cannot update metadata in device library."),
            _("No fetch queries found for firmware version."),
            show=True,
        )
        return

    options = ReadLocationsJobOptions(
        cfg.plugin_prefs.BookmarkOptions,
        device.epub_location_like_kepub,
        fetch_queries,
        device.db_path,
        device.device_db_path,
        device.is_db_copied,
        profile_name,
        None,
        device.supports_ratings,
        allOnDevice=False,
        prompt_to_store=True,
    )
    debug("options:", options)

    if cfg.plugin_prefs.BookmarkOptions.backgroundJob:
        ReadLocationsProgressDialog(
            gui,
            device,
            dispatcher,
            options,
            current_view.model().db,
        )
    else:
        selectedIDs = utils.get_selected_ids(gui)

        if len(selectedIDs) == 0:
            return
        debug("selectedIDs:", selectedIDs)
        books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)
        for book in books:
            device_book_paths = utils.get_device_paths_from_id(
                cast("int", book.calibre_id), gui
            )
            book.paths = device_book_paths
            book.contentIDs = [
                utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
                for path in device_book_paths
            ]

        reading_locations_updated, books_without_reading_locations, count_books = (
            _store_current_bookmark(books, device, gui, options)
        )
        result_message = (
            _("Update summary:")
            + "\n\t"
            + _(
                "Reading locations updated={0}\n\tBooks with no reading location={1}\n\tTotal books checked={2}"
            ).format(
                reading_locations_updated,
                books_without_reading_locations,
                count_books,
            )
        )
        info_dialog(
            gui,
            _("Kobo Utilities") + " - " + _("Library updated"),
            result_message,
            show=True,
        )


def _store_current_bookmark(
    books: list[Book],
    device: KoboDevice,
    gui: ui.Main,
    options: ReadLocationsJobOptions,
):
    reading_locations_updated = 0
    books_without_reading_locations = 0
    count_books = 0

    def value_changed(old_value: object | None, new_value: object | None):
        return (
            (old_value is not None and new_value is None)
            or (old_value is None and new_value is not None)
            or old_value != new_value
        )

    debug("profile_name=", options.profile_name)
    clear_if_unread = options.bookmark_options.clearIfUnread
    store_if_more_recent = options.bookmark_options.storeIfMoreRecent
    do_not_store_if_reopened = options.bookmark_options.doNotStoreIfReopened

    connection = utils.device_database_connection(device, use_row_factory=True)
    progressbar = ProgressBar(
        parent=gui, window_title=_("Storing reading positions"), on_top=True
    )
    progressbar.show_with_maximum(len(books))

    library_db = gui.current_db
    custom_columns = cfg.get_column_names(gui, device, options.profile_name)
    kobo_chapteridbookmarked_column_name = custom_columns.current_location
    kobo_percentRead_column_name = custom_columns.percent_read
    rating_column_name = custom_columns.rating
    last_read_column_name = custom_columns.last_read
    time_spent_reading_column_name = custom_columns.time_spent_reading
    rest_of_book_estimate_column_name = custom_columns.rest_of_book_estimate

    debug(
        "kobo_chapteridbookmarked_column_name=",
        kobo_chapteridbookmarked_column_name,
    )
    debug("kobo_percentRead_column_name=", kobo_percentRead_column_name)
    debug("rating_column_name=", rating_column_name)
    debug("last_read_column_name=", last_read_column_name)
    debug("time_spent_reading_column_name=", time_spent_reading_column_name)
    debug("rest_of_book_estimate_column_name=", rest_of_book_estimate_column_name)

    rating_col_label = None
    if rating_column_name is not None:
        if rating_column_name != "rating":
            rating_col_label = (
                library_db.field_metadata.key_to_label(rating_column_name)
                if rating_column_name
                else ""
            )
        debug("rating_col_label=", rating_col_label)

    id_map = {}
    id_map_percentRead = {}
    id_map_chapteridbookmarked = {}
    id_map_rating = {}
    id_map_last_read = {}
    id_map_time_spent_reading = {}
    id_map_rest_of_book_estimate = {}

    debug("Starting to look at selected books...")
    cursor = connection.cursor()
    for book in books:
        count_books += 1
        mi = Metadata("Unknown")
        debug("Looking at book: %s" % book.title)
        progressbar.set_label(_("Checking {}").format(book.title))
        progressbar.increment()
        book_updated = False

        if len(cast("list[str]", book.contentIDs)) == 0:
            books_without_reading_locations += 1
            continue

        for contentID in cast("list[str]", book.contentIDs):
            debug("contentId='%s'" % (contentID))
            fetch_values = (contentID,)
            assert device.driver.fwversion is not None
            fetch_queries = _get_fetch_query_for_firmware_version(
                cast("tuple[int, int, int]", device.driver.fwversion)
            )
            assert fetch_queries is not None
            if contentID.endswith(".kepub.epub"):
                fetch_query = fetch_queries.kepub
            else:
                fetch_query = fetch_queries.epub
            debug("fetch_query='%s'" % (fetch_query))
            cursor.execute(fetch_query, fetch_values)
            try:
                result = next(cursor)
            except StopIteration:
                result = None

            kobo_chapteridbookmarked = None
            kobo_adobe_location = None
            kobo_percentRead = None
            last_read = None
            time_spent_reading = None
            rest_of_book_estimate = None

            if result is not None:
                debug("result=", result)
                if result["ReadStatus"] == 0:
                    if clear_if_unread:
                        kobo_chapteridbookmarked = None
                        kobo_adobe_location = None
                        kobo_percentRead = None
                        last_read = None
                        kobo_rating = 0
                        time_spent_reading = None
                        rest_of_book_estimate = None
                    else:
                        books_without_reading_locations += 1
                        continue
                else:
                    if result["DateLastRead"]:
                        debug("result['DateLastRead']=", result["DateLastRead"])
                        last_read = utils.convert_kobo_date(result["DateLastRead"])
                        debug("last_read=", last_read)

                    if last_read_column_name is not None and store_if_more_recent:
                        metadata = book.get_user_metadata(last_read_column_name, True)
                        assert metadata is not None
                        current_last_read = metadata["#value#"]
                        debug(
                            "book.get_user_metadata(last_read_column_name, True)['#value#']=",
                            current_last_read,
                        )
                        debug("setting mi.last_read=", last_read)
                        if current_last_read is not None and last_read is not None:
                            debug(
                                "store_if_more_recent - current_last_read < last_read=",
                                current_last_read < last_read,
                            )
                            if current_last_read >= last_read:
                                continue
                        elif current_last_read is not None and last_read is None:
                            continue

                    if (
                        kobo_percentRead_column_name is not None
                        and do_not_store_if_reopened
                    ):
                        metadata = book.get_user_metadata(
                            kobo_percentRead_column_name, True
                        )
                        assert metadata is not None
                        current_percentRead = metadata["#value#"]
                        debug(
                            "do_not_store_if_reopened - current_percentRead=",
                            current_percentRead,
                        )
                        if (
                            current_percentRead is not None
                            and current_percentRead >= 100
                        ):
                            continue

                    if (
                        result["MimeType"] == MIMETYPE_KOBO
                        or device.epub_location_like_kepub
                    ):
                        kobo_chapteridbookmarked = result["ChapterIDBookmarked"]
                        kobo_adobe_location = None
                    else:
                        kobo_chapteridbookmarked = (
                            result["ChapterIDBookmarked"][len(contentID) + 1 :]
                            if result["ChapterIDBookmarked"]
                            else None
                        )
                        kobo_adobe_location = result["adobe_location"]

                    if result["ReadStatus"] == 1:
                        kobo_percentRead = result["___PercentRead"]
                    elif result["ReadStatus"] == 2:
                        kobo_percentRead = 100

                    kobo_rating = result["Rating"] * 2 if result["Rating"] else 0

                    time_spent_reading = result.get("TimeSpentReading")
                    rest_of_book_estimate = result.get("RestOfBookEstimate")

            else:
                books_without_reading_locations += 1
                continue

            debug("kobo_chapteridbookmarked='%s'" % (kobo_chapteridbookmarked))
            debug("kobo_adobe_location='%s'" % (kobo_adobe_location))
            debug("kobo_percentRead=", kobo_percentRead)
            debug("time_spent_reading='%s'" % (time_spent_reading))
            debug("rest_of_book_estimate='%s'" % (rest_of_book_estimate))

            if last_read_column_name is not None:
                metadata = book.get_user_metadata(last_read_column_name, True)
                assert metadata is not None
                current_last_read = metadata["#value#"]
                debug(
                    "book.get_user_metadata(last_read_column_name, True)['#value#']=",
                    current_last_read,
                )
                debug("setting mi.last_read=", last_read)
                debug(
                    "current_last_read == last_read=",
                    current_last_read == last_read,
                )

                if value_changed(current_last_read, last_read):
                    id_map_last_read[book.calibre_id] = last_read
                    book_updated = True
                else:
                    book_updated = book_updated or False

            if kobo_chapteridbookmarked_column_name is not None:
                if (
                    kobo_chapteridbookmarked is not None
                    and kobo_adobe_location is not None
                ):
                    new_value = (
                        kobo_chapteridbookmarked
                        + BOOKMARK_SEPARATOR
                        + kobo_adobe_location
                    )
                elif kobo_chapteridbookmarked:
                    new_value = kobo_chapteridbookmarked
                else:
                    new_value = None
                    debug("setting bookmark column to None")
                debug("chapterIdBookmark - on kobo=", new_value)
                metadata = book.get_user_metadata(
                    kobo_chapteridbookmarked_column_name, True
                )
                assert metadata is not None
                old_value = metadata["#value#"]
                debug("chapterIdBookmark - in library=", old_value)
                debug(
                    "chapterIdBookmark - on kobo==in library=",
                    new_value == old_value,
                )

                if value_changed(old_value, new_value):
                    id_map_chapteridbookmarked[book.calibre_id] = new_value
                    book_updated = True
                else:
                    book_updated = book_updated or False

            if kobo_percentRead_column_name is not None:
                debug("setting kobo_percentRead=", kobo_percentRead)
                metadata = book.get_user_metadata(kobo_percentRead_column_name, True)
                assert metadata is not None
                current_percentRead = metadata["#value#"]
                debug("percent read - in book=", current_percentRead)

                if value_changed(current_percentRead, kobo_percentRead):
                    id_map_percentRead[book.calibre_id] = kobo_percentRead
                    book_updated = True
                else:
                    book_updated = book_updated or False

            if rating_column_name is not None and kobo_rating > 0:
                debug("setting rating_column_name=", rating_column_name)
                if rating_column_name == "rating":
                    current_rating = book.rating
                    debug("rating - in book=", current_rating)
                    if current_rating != kobo_rating:
                        library_db.set_rating(
                            book.calibre_id, kobo_rating, commit=False
                        )
                else:
                    metadata = book.get_user_metadata(rating_column_name, True)
                    assert metadata is not None
                    current_rating = metadata["#value#"]
                    if current_rating != kobo_rating:
                        library_db.set_custom(
                            book.calibre_id,
                            kobo_rating,
                            label=rating_col_label,
                            commit=False,
                        )
                if value_changed(current_rating, kobo_rating):
                    id_map_rating[book.calibre_id] = kobo_rating
                    book_updated = True
                else:
                    book_updated = book_updated or False

            if time_spent_reading_column_name is not None:
                debug("setting time_spent_reading=", time_spent_reading)
                metadata = book.get_user_metadata(time_spent_reading_column_name, True)
                assert metadata is not None
                current_time_spent_reading = metadata["#value#"]
                debug("time spent reading - in book=", current_time_spent_reading)

                if value_changed(current_time_spent_reading, time_spent_reading):
                    id_map_time_spent_reading[book.calibre_id] = time_spent_reading
                    book_updated = True
                else:
                    book_updated = book_updated or False

            if rest_of_book_estimate_column_name is not None:
                debug("setting rest_of_book_estimate=", rest_of_book_estimate)
                metadata = book.get_user_metadata(time_spent_reading_column_name, True)
                assert metadata is not None
                current_rest_of_book_estimate = metadata["#value#"]
                debug(
                    "rest of book estimate - in book=",
                    current_rest_of_book_estimate,
                )

                if value_changed(current_rest_of_book_estimate, rest_of_book_estimate):
                    id_map_rest_of_book_estimate[book.calibre_id] = (
                        rest_of_book_estimate
                    )
                    book_updated = True
                else:
                    book_updated = book_updated or False

            id_map[book.calibre_id] = mi

        if book_updated:
            reading_locations_updated += 1

    debug("Updating GUI - new DB engine")
    if kobo_chapteridbookmarked_column_name and len(id_map_chapteridbookmarked) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (
                kobo_chapteridbookmarked_column_name,
                len(id_map_chapteridbookmarked),
            )
        )
        library_db.new_api.set_field(
            kobo_chapteridbookmarked_column_name, id_map_chapteridbookmarked
        )
    if kobo_percentRead_column_name and len(id_map_percentRead) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (kobo_percentRead_column_name, len(id_map_percentRead))
        )
        library_db.new_api.set_field(kobo_percentRead_column_name, id_map_percentRead)
    if rating_column_name and len(id_map_rating) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (rating_column_name, len(id_map_rating))
        )
        library_db.new_api.set_field(rating_column_name, id_map_rating)
    if last_read_column_name and len(id_map_last_read) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (last_read_column_name, len(id_map_last_read))
        )
        library_db.new_api.set_field(last_read_column_name, id_map_last_read)
    if time_spent_reading_column_name and len(id_map_time_spent_reading) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (time_spent_reading_column_name, len(id_map_time_spent_reading))
        )
        library_db.new_api.set_field(
            time_spent_reading_column_name, id_map_time_spent_reading
        )
    if rest_of_book_estimate_column_name and len(id_map_rest_of_book_estimate) > 0:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (
                rest_of_book_estimate_column_name,
                len(id_map_rest_of_book_estimate),
            )
        )
        library_db.new_api.set_field(
            rest_of_book_estimate_column_name, id_map_rest_of_book_estimate
        )
    gui.iactions["Edit Metadata"].refresh_gui(list(id_map))

    progressbar.hide()
    if len(id_map) > 0:
        gui.status_bar.show_message(
            _("Kobo Utilities")
            + " - "
            + _("Storing reading positions completed - {0} changed.").format(
                len(id_map)
            ),
            3000,
        )

    library_db.commit()

    debug("finished")

    return (reading_locations_updated, books_without_reading_locations, count_books)


def restore_current_bookmark(
    device: KoboDevice, gui: ui.Main, profile_name: str
) -> None:
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    selectedIDs = utils.get_selected_ids(gui)

    if len(selectedIDs) == 0:
        return
    debug("selectedIDs:", selectedIDs)
    books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)
    for book in books:
        device_book_paths = utils.get_device_paths_from_id(
            cast("int", book.calibre_id), gui
        )
        debug("device_book_paths:", device_book_paths)
        book.paths = device_book_paths
        book.contentIDs = [
            utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
            for path in device_book_paths
        ]

    updated_books, not_on_device_books, count_books = _restore_current_bookmark(
        books, device, gui, cfg.plugin_prefs.BookmarkOptions, profile_name
    )
    result_message = (
        _("Update summary:")
        + "\n\t"
        + _("Books updated={0}\n\tBooks not on device={1}\n\tTotal books={2}").format(
            updated_books, not_on_device_books, count_books
        )
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Device library updated"),
        result_message,
        show=True,
    )


def _restore_current_bookmark(
    books: list[Book],
    device: KoboDevice,
    gui: ui.Main,
    options: cfg.BookmarkOptionsConfig,
    profile_name: str | None,
):
    updated_books = 0
    not_on_device_books = 0
    count_books = 0

    custom_columns = cfg.get_column_names(gui, device, profile_name)
    kobo_chapteridbookmarked_column = custom_columns.current_location
    kobo_percentRead_column = custom_columns.percent_read
    rating_column = custom_columns.rating
    last_read_column = custom_columns.last_read
    time_spent_reading_column = custom_columns.time_spent_reading
    rest_of_book_estimate_column = custom_columns.rest_of_book_estimate
    chapter_query = (
        "SELECT c1.ChapterIDBookmarked, "
        "c1.ReadStatus, "
        "c1.___PercentRead, "
        "c1.Attribution, "
        "c1.DateLastRead, "
        "c1.___SyncTime, "
        "c1.Title, "
        "c1.MimeType, "
        "c1.TimeSpentReading, "
        "c1.RestOfBookEstimate, "
    )
    if device.supports_ratings:
        chapter_query += " r.Rating, r.DateModified "
    else:
        chapter_query += " NULL as Rating, NULL as DateModified "
    chapter_query += "FROM content c1 "
    if device.supports_ratings:
        chapter_query += " left outer join ratings r on c1.ContentID = r.ContentID "
    chapter_query += "WHERE c1.BookId IS NULL AND c1.ContentId = ?"
    debug("chapter_query= ", chapter_query)

    volume_zero_query = (
        "SELECT contentID FROM content WHERE BookId = ? and VolumeIndex = 0"
    )

    chapter_update = (
        "UPDATE content "
        "SET ChapterIDBookmarked = ? "
        "  , FirstTimeReading = ? "
        "  , ReadStatus = ? "
        "  , ___PercentRead = ? "
        "  , DateLastRead = ? "
        "  , TimeSpentReading = ? "
        "  , RestOfBookEstimate = ? "
        "WHERE BookID IS NULL "
        "AND ContentID = ?"
    )
    location_update = (
        "UPDATE content SET adobe_location = ? WHERE ContentType = 9 AND ContentID = ?"
    )
    rating_update = (
        "UPDATE ratings "
        "SET Rating = ?, "
        "DateModified = ? "
        "WHERE ContentID  = ?"
    )  # fmt: skip
    rating_insert = (
        "INSERT INTO ratings ("
        "Rating, "
        "DateModified, "
        "ContentID "
        ")"
        "VALUES (?, ?, ?)"
    )  # fmt: skip
    rating_delete = "DELETE FROM ratings WHERE ContentID = ?"

    with utils.device_database_connection(device, use_row_factory=True) as connection:
        cursor = connection.cursor()
        for book in books:
            count_books += 1
            for contentID in cast("list[str]", book.contentIDs):
                chapter_values = (contentID,)
                cursor.execute(chapter_query, chapter_values)
                try:
                    result = next(cursor)
                except StopIteration:
                    result = None

                if result is not None:
                    debug("result= ", result)
                    chapter_update = "UPDATE content SET "
                    chapter_set_clause = ""
                    chapter_values = []
                    location_update = "UPDATE content SET "
                    location_set_clause = ""
                    location_values = []
                    rating_change_query = None
                    rating_values = []

                    kobo_chapteridbookmarked = None
                    kobo_adobe_location = None
                    kobo_percentRead = None
                    kobo_time_spent_reading = None
                    kobo_rest_of_book_estimate = None

                    if kobo_chapteridbookmarked_column:
                        metadata = book.get_user_metadata(
                            kobo_chapteridbookmarked_column, True
                        )
                        assert metadata is not None
                        reading_location_string = metadata["#value#"]
                        debug("reading_location_string=", reading_location_string)
                        if reading_location_string is not None:
                            if result["MimeType"] == MIMETYPE_KOBO:
                                kobo_chapteridbookmarked = reading_location_string
                                kobo_adobe_location = None
                            else:
                                reading_location_parts = reading_location_string.split(
                                    BOOKMARK_SEPARATOR
                                )
                                debug(
                                    "reading_location_parts=",
                                    reading_location_parts,
                                )
                                debug(
                                    "device.epub_location_like_kepub=",
                                    device.epub_location_like_kepub,
                                )
                                if device.epub_location_like_kepub:
                                    kobo_chapteridbookmarked = (
                                        reading_location_parts[1]
                                        if len(reading_location_parts) == 2
                                        else reading_location_string
                                    )
                                    kobo_adobe_location = None
                                else:
                                    if len(reading_location_parts) == 2:
                                        kobo_chapteridbookmarked = (
                                            contentID + "#" + reading_location_parts[0]
                                        )
                                        kobo_adobe_location = reading_location_parts[1]
                                    else:
                                        cursor.execute(volume_zero_query, [contentID])
                                        try:
                                            volume_zero_result = next(cursor)
                                            kobo_chapteridbookmarked = (
                                                volume_zero_result["ContentID"]
                                            )
                                            kobo_adobe_location = (
                                                reading_location_parts[0]
                                            )
                                        except StopIteration:
                                            volume_zero_result = None

                        if reading_location_string:
                            chapter_values.append(kobo_chapteridbookmarked)
                            chapter_set_clause += ", ChapterIDBookmarked  = ? "
                            location_values.append(kobo_adobe_location)
                            location_set_clause += ", adobe_location  = ? "
                        else:
                            debug("reading_location_string=", reading_location_string)

                    if kobo_percentRead_column:
                        metadata = book.get_user_metadata(kobo_percentRead_column, True)
                        assert metadata is not None
                        kobo_percentRead = metadata["#value#"]
                        kobo_percentRead = kobo_percentRead or result["___PercentRead"]
                        chapter_values.append(kobo_percentRead)
                        chapter_set_clause += ", ___PercentRead  = ? "

                    if options.readingStatus and kobo_percentRead:
                        debug("chapter_values= ", chapter_values)
                        if kobo_percentRead == 100:
                            chapter_values.append(2)
                            debug("chapter_values= ", chapter_values)
                        else:
                            chapter_values.append(1)
                            debug("chapter_values= ", chapter_values)
                        chapter_set_clause += ", ReadStatus  = ? "
                        chapter_values.append("false")
                        chapter_set_clause += ", FirstTimeReading = ? "

                    last_read = None
                    if options.setDateToNow:
                        last_read = strftime(device.timestamp_string, time.gmtime())
                        debug("setting to now - last_read= ", last_read)
                    elif last_read_column:
                        metadata = book.get_user_metadata(last_read_column, True)
                        assert metadata is not None
                        last_read = metadata["#value#"]
                        if last_read is not None:
                            last_read = last_read.strftime(device.timestamp_string)
                        debug("setting from library - last_read= ", last_read)
                    debug("last_read= ", last_read)
                    debug("result['___SyncTime']= ", result["___SyncTime"])
                    if last_read is not None:
                        chapter_values.append(last_read)
                        chapter_set_clause += ", DateLastRead  = ? "
                        # Somewhere the "Recent" sort changed from only using the ___SyncTime if DateLastRead was null,
                        # Now it uses the MAX(___SyncTime, DateLastRead). Need to set ___SyncTime if it is after DateLastRead
                        # to correctly maintain sort order.
                        if (
                            device.driver.fwversion >= (4, 1, 0)
                            and last_read < result["___SyncTime"]
                        ):
                            debug("setting ___SyncTime to same as DateLastRead")
                            chapter_values.append(last_read)
                            chapter_set_clause += ", ___SyncTime  = ? "

                    debug("options.rating= ", options.rating)
                    rating = None
                    if rating_column is not None and options.rating:
                        if rating_column == "rating":
                            rating = book.rating
                        else:
                            metadata = book.get_user_metadata(rating_column, True)
                            assert metadata is not None
                            rating = metadata["#value#"]
                        assert isinstance(rating, int)
                        rating = None if not rating or rating == 0 else rating / 2
                        debug(
                            "rating=",
                            rating,
                            " result['Rating']=",
                            result["Rating"],
                        )
                        rating_values.append(rating)
                        if last_read is not None:
                            rating_values.append(last_read)
                        else:
                            rating_values.append(
                                strftime(device.timestamp_string, time.gmtime())
                            )

                        rating_values.append(contentID)
                        if rating is None:
                            rating_change_query = rating_delete
                            rating_values = (contentID,)
                        elif (
                            result["DateModified"] is None
                        ):  # If the date modified column does not have a value, there is no rating column
                            rating_change_query = rating_insert
                        else:
                            rating_change_query = rating_update

                    if time_spent_reading_column:
                        metadata = book.get_user_metadata(
                            time_spent_reading_column, True
                        )
                        assert metadata is not None
                        kobo_time_spent_reading = metadata["#value#"]
                        kobo_time_spent_reading = (
                            kobo_time_spent_reading
                            if kobo_time_spent_reading is not None
                            else 0
                        )
                        chapter_values.append(kobo_time_spent_reading)
                        chapter_set_clause += ", TimeSpentReading = ? "

                    if rest_of_book_estimate_column:
                        metadata = book.get_user_metadata(
                            rest_of_book_estimate_column, True
                        )
                        assert metadata is not None
                        kobo_rest_of_book_estimate = metadata["#value#"]
                        kobo_rest_of_book_estimate = (
                            kobo_rest_of_book_estimate
                            if kobo_rest_of_book_estimate is not None
                            else 0
                        )
                        chapter_values.append(kobo_rest_of_book_estimate)
                        chapter_set_clause += ", RestOfBookEstimate = ? "

                    debug("found contentId='%s'" % (contentID))
                    debug("kobo_chapteridbookmarked=", kobo_chapteridbookmarked)
                    debug("kobo_adobe_location=", kobo_adobe_location)
                    debug("kobo_percentRead=", kobo_percentRead)
                    debug("rating=", rating)
                    debug("last_read=", last_read)
                    debug("kobo_time_spent_reading=", kobo_time_spent_reading)
                    debug("kobo_rest_of_book_estimate=", kobo_rest_of_book_estimate)

                    if len(chapter_set_clause) > 0:
                        chapter_update += chapter_set_clause[1:]
                        chapter_update += "WHERE ContentID = ? AND BookID IS NULL"
                        chapter_values.append(contentID)
                    else:
                        debug(
                            "no changes found to selected metadata. No changes being made."
                        )
                        not_on_device_books += 1
                        continue

                    debug("chapter_update=%s" % chapter_update)
                    debug("chapter_values= ", chapter_values)
                    try:
                        cursor.execute(chapter_update, chapter_values)
                        if len(location_set_clause) > 0 and not (
                            result["MimeType"] == MIMETYPE_KOBO
                            or device.epub_location_like_kepub
                        ):
                            location_update += location_set_clause[1:]
                            location_update += (
                                " WHERE ContentID = ? AND BookID IS NOT NULL"
                            )
                            location_values.append(kobo_chapteridbookmarked)
                            debug("location_update=%s" % location_update)
                            debug("location_values= ", location_values)
                            cursor.execute(location_update, location_values)
                        if rating_change_query:
                            debug("rating_change_query=%s" % rating_change_query)
                            debug("rating_values= ", rating_values)
                            cursor.execute(rating_change_query, rating_values)

                        updated_books += 1
                    except:
                        debug("    Database Exception:  Unable to set bookmark info.")
                        raise
                else:
                    debug(
                        "no match for title='%s' contentId='%s'"
                        % (book.title, book.contentID)
                    )
                    not_on_device_books += 1
    debug(
        "Update summary: Books updated=%d, not on device=%d, Total=%d"
        % (updated_books, not_on_device_books, count_books)
    )

    return (updated_books, not_on_device_books, count_books)


def auto_store_current_bookmark(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
):
    del load_resources
    debug("start")

    library_db = gui.current_db

    fetch_queries = _get_fetch_query_for_firmware_version(
        cast("tuple[int, int, int]", device.driver.fwversion)
    )
    assert fetch_queries is not None
    profile = device.profile
    custom_columns = cfg.get_column_names(gui, device)

    bookmark_options = cfg.BookmarkOptionsConfig()
    bookmark_options.backgroundJob = True
    bookmark_options.clearIfUnread = False
    bookmark_options.storeBookmarks = True

    profile_name = None
    prompt_to_store = False
    if profile is not None:
        profile_name = profile.profileName
        prompt_to_store = profile.storeOptionsStore.promptToStore
        bookmark_options.storeIfMoreRecent = profile.storeOptionsStore.storeIfMoreRecent
        bookmark_options.doNotStoreIfReopened = (
            profile.storeOptionsStore.doNotStoreIfReopened
        )
    options = ReadLocationsJobOptions(
        bookmark_options,
        device.epub_location_like_kepub,
        fetch_queries,
        device.db_path,
        device.device_db_path,
        device.is_db_copied,
        profile_name,
        custom_columns,
        device.supports_ratings,
        allOnDevice=True,
        prompt_to_store=prompt_to_store,
    )

    kobo_chapteridbookmarked_column = custom_columns.current_location
    kobo_percentRead_column = custom_columns.percent_read
    rating_column = custom_columns.rating
    last_read_column = custom_columns.last_read
    time_spent_reading_column = custom_columns.time_spent_reading
    rest_of_book_estimate_column = custom_columns.rest_of_book_estimate

    if options.bookmark_options.doNotStoreIfReopened:
        search_condition = (
            f"and ({kobo_percentRead_column}:false or {kobo_percentRead_column}:<100)"
        )
    else:
        search_condition = ""

    progressbar = ProgressBar(
        parent=gui,
        window_title=_("Queuing books for storing reading position"),
    )
    progressbar.set_label(_("Getting list of books"))
    progressbar.show_with_maximum(0)

    search_condition = f"ondevice:True {search_condition}"
    debug("search_condition=", search_condition)
    onDeviceIds = set(
        library_db.search_getting_ids(
            search_condition, None, sort_results=False, use_virtual_library=False
        )
    )
    debug("onDeviceIds:", len(onDeviceIds))
    onDevice_book_paths = utils.get_books_from_ids(onDeviceIds, gui)
    debug("onDevice_book_paths:", len(onDevice_book_paths))

    books = utils.convert_calibre_ids_to_books(library_db, onDeviceIds)
    progressbar.show_with_maximum(len(books))
    progressbar.set_label(_("Queuing books"))
    books_to_scan = []

    for book in books:
        progressbar.increment()
        device_book_paths = [
            x.path for x in onDevice_book_paths[cast("int", book.calibre_id)]
        ]
        book.contentIDs = [
            utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
            for path in device_book_paths
        ]
        if len(book.contentIDs) > 0:
            title = book.title
            progressbar.set_label(_("Queueing {}").format(title))
            authors = authors_to_string(book.authors)
            current_chapterid = None
            current_percentRead = None
            current_rating = None
            current_last_read = None
            current_time_spent_reading = None
            current_rest_of_book_estimate = None
            if kobo_chapteridbookmarked_column is not None:
                metadata = book.get_user_metadata(
                    kobo_chapteridbookmarked_column, False
                )
                assert metadata is not None
                current_chapterid = metadata["#value#"]
            if kobo_percentRead_column is not None:
                metadata = book.get_user_metadata(kobo_percentRead_column, False)
                assert metadata is not None
                current_percentRead = metadata["#value#"]
            if rating_column is not None:
                if rating_column == "rating":
                    current_rating = book.rating
                else:
                    metadata = book.get_user_metadata(rating_column, False)
                    assert metadata is not None
                    current_rating = metadata["#value#"]
            if last_read_column is not None:
                metadata = book.get_user_metadata(last_read_column, False)
                assert metadata is not None
                current_last_read = metadata["#value#"]
            if time_spent_reading_column is not None:
                metadata = book.get_user_metadata(time_spent_reading_column, False)
                assert metadata is not None
                current_time_spent_reading = metadata["#value#"]
            if rest_of_book_estimate_column is not None:
                metadata = book.get_user_metadata(rest_of_book_estimate_column, False)
                assert metadata is not None
                current_rest_of_book_estimate = metadata["#value#"]

            books_to_scan.append(
                (
                    book.calibre_id,
                    book.contentIDs,
                    title,
                    authors,
                    current_chapterid,
                    current_percentRead,
                    current_rating,
                    current_last_read,
                    current_time_spent_reading,
                    current_rest_of_book_estimate,
                )
            )

    if len(books_to_scan) > 0:
        _store_queue_job(dispatcher, device, gui, options, books_to_scan)

    progressbar.hide()

    debug("Finish")


def _store_queue_job(
    dispatcher: Dispatcher,
    device: KoboDevice,
    gui: ui.Main,
    options: ReadLocationsJobOptions,
    books_to_modify: list[tuple[Any]],
):
    debug("Start")
    cpus = 1  # self.gui.device_manager.server.pool_size

    args = [books_to_modify, options, cpus]
    desc = _("Storing reading positions for {0} books").format(len(books_to_modify))
    gui.device_manager.create_job(
        do_read_locations,
        dispatcher(partial(_read_completed, device=device, gui=gui)),
        description=desc,
        args=args,
    )
    gui.status_bar.show_message(GUI_NAME + " - " + desc, 3000)


def _read_completed(job: DeviceJob, device: KoboDevice, gui: ui.Main):
    if job.failed:
        gui.job_exception(job, dialog_title=_("Failed to get reading positions"))
        return
    modified_epubs_map: dict[int, dict[str, Any]]
    options: ReadLocationsJobOptions
    modified_epubs_map, options = job.result
    debug("options", options)

    update_count = len(modified_epubs_map) if modified_epubs_map else 0
    if update_count == 0:
        gui.status_bar.show_message(
            _("Kobo Utilities")
            + " - "
            + _("Storing reading positions completed - No changes found"),
            3000,
        )
    else:
        goodreads_sync_plugin = None
        if options.prompt_to_store:
            profile_name = options.profile_name
            db = gui.current_db

            if "Goodreads Sync" in gui.iactions:
                goodreads_sync_plugin = gui.iactions["Goodreads Sync"]

            dlg = ShowReadingPositionChangesDialog(
                gui,
                modified_epubs_map,
                device,
                db,
                profile_name,
                goodreads_sync_plugin is not None,
            )
            dlg.exec()
            if dlg.result() != dlg.DialogCode.Accepted:
                debug("dialog cancelled")
                return
            modified_epubs_map = dlg.reading_locations
        _update_database_columns(modified_epubs_map, device, gui)

        if options.prompt_to_store:
            library_config = cfg.get_library_config(gui.current_db)
            if (
                library_config.readingPositionChangesStore.selectBooksInLibrary
                or library_config.readingPositionChangesStore.updeateGoodreadsProgress
            ):
                gui.library_view.select_rows(list(modified_epubs_map.keys()))
            if (
                goodreads_sync_plugin
                and library_config.readingPositionChangesStore.updeateGoodreadsProgress
            ):
                debug(
                    "goodreads_sync_plugin.users.keys()=",
                    list(goodreads_sync_plugin.users.keys()),
                )
                goodreads_sync_plugin.update_reading_progress(
                    "progress", sorted(goodreads_sync_plugin.users.keys())[0]
                )


def _update_database_columns(
    reading_locations: dict[int, dict[str, Any]], device: KoboDevice, gui: ui.Main
):
    debug("reading_locations=", reading_locations)
    debug("start number of reading_locations= %d" % (len(reading_locations)))
    progressbar = ProgressBar(
        parent=gui, window_title=_("Storing reading positions"), on_top=True
    )
    total_books = len(reading_locations)
    progressbar.show_with_maximum(total_books)

    library_db = gui.current_db

    def value_changed(old_value: object | None, new_value: object | None):
        return bool(
            (old_value is not None and new_value is None)
            or (old_value is None and new_value is not None)
            or old_value != new_value
        )

    custom_columns = cfg.get_column_names(gui, device)
    kobo_chapteridbookmarked_column_name = custom_columns.current_location
    kobo_percentRead_column_name = custom_columns.percent_read
    rating_column_name = custom_columns.rating
    last_read_column_name = custom_columns.last_read
    time_spent_reading_column_name = custom_columns.time_spent_reading
    rest_of_book_estimate_column_name = custom_columns.rest_of_book_estimate

    if kobo_chapteridbookmarked_column_name is not None:
        debug(
            "kobo_chapteridbookmarked_column_name=",
            kobo_chapteridbookmarked_column_name,
        )
        kobo_chapteridbookmarked_col_label = library_db.field_metadata.key_to_label(
            kobo_chapteridbookmarked_column_name
        )
        debug(
            "kobo_chapteridbookmarked_col_label=",
            kobo_chapteridbookmarked_col_label,
        )

    debug(
        "kobo_chapteridbookmarked_column_name=",
        kobo_chapteridbookmarked_column_name,
    )
    debug(
        "_update_database_columns - kobo_percentRead_column_name=",
        kobo_percentRead_column_name,
    )
    debug("rating_column_name=", rating_column_name)
    debug("last_read_column_name=", last_read_column_name)
    debug("time_spent_reading_column_name=", time_spent_reading_column_name)
    debug("rest_of_book_estimate_column_name=", rest_of_book_estimate_column_name)
    # At this point we want to re-use code in edit_metadata to go ahead and
    # apply the changes. So we will create empty Metadata objects so only
    # the custom column field gets updated
    id_map = {}
    id_map_percentRead = {}
    id_map_chapteridbookmarked = {}
    id_map_rating = {}
    id_map_last_read = {}
    id_map_time_spent_reading = {}
    id_map_rest_of_book_estimate = {}
    for book_id, reading_location in list(reading_locations.items()):
        mi = Metadata(_("Unknown"))
        book_mi = library_db.get_metadata(book_id, index_is_id=True, get_cover=False)
        book = Book("", "lpath", title=book_mi.title, other=book_mi)
        progressbar.set_label(_("Updating {}").format(book_mi.title))
        progressbar.increment()

        kobo_chapteridbookmarked = None
        kobo_adobe_location = None
        kobo_percentRead = None
        last_read = None
        time_spent_reading = None
        rest_of_book_estimate = None
        debug("reading_location=", reading_location)
        if (
            reading_location["MimeType"] == MIMETYPE_KOBO
            or device.epub_location_like_kepub
        ):
            kobo_chapteridbookmarked = reading_location["ChapterIDBookmarked"]
            kobo_adobe_location = None
        else:
            kobo_chapteridbookmarked = (
                reading_location["ChapterIDBookmarked"][
                    len(reading_location["ContentID"]) + 1 :
                ]
                if reading_location["ChapterIDBookmarked"]
                else None
            )
            kobo_adobe_location = reading_location["adobe_location"]

        if reading_location["ReadStatus"] == 1:
            kobo_percentRead = reading_location["___PercentRead"]
        elif reading_location["ReadStatus"] == 2:
            kobo_percentRead = 100

        if reading_location["Rating"]:
            kobo_rating = reading_location["Rating"] * 2
        else:
            kobo_rating = 0

        if reading_location["DateLastRead"]:
            last_read = utils.convert_kobo_date(reading_location["DateLastRead"])

        if reading_location["TimeSpentReading"]:
            time_spent_reading = reading_location["TimeSpentReading"]

        if reading_location["RestOfBookEstimate"]:
            rest_of_book_estimate = reading_location["RestOfBookEstimate"]

        book_updated = False
        if last_read_column_name is not None:
            last_read_metadata = book.get_user_metadata(last_read_column_name, True)
            assert last_read_metadata is not None
            current_last_read = last_read_metadata["#value#"]
            debug(
                "book.get_user_metadata(last_read_column_name, True)['#value#']=",
                current_last_read,
            )
            debug("setting mi.last_read=", last_read)
            debug("current_last_read == last_read=", current_last_read == last_read)

            if value_changed(current_last_read, last_read):
                id_map_last_read[book_id] = last_read
                book_updated = True
            else:
                book_updated = book_updated or False

        if kobo_chapteridbookmarked_column_name is not None:
            debug("kobo_chapteridbookmarked='%s'" % (kobo_chapteridbookmarked))
            debug("kobo_adobe_location='%s'" % (kobo_adobe_location))
            debug("kobo_percentRead=", kobo_percentRead)
            if kobo_chapteridbookmarked is not None and kobo_adobe_location is not None:
                new_value = (
                    kobo_chapteridbookmarked + BOOKMARK_SEPARATOR + kobo_adobe_location
                )
            elif kobo_chapteridbookmarked:
                new_value = kobo_chapteridbookmarked
            else:
                new_value = None
                debug("setting bookmark column to None")
            debug("chapterIdBookmark - on kobo=", new_value)
            metadata = book.get_user_metadata(
                kobo_chapteridbookmarked_column_name, True
            )
            assert metadata is not None
            old_value = metadata["#value#"]
            debug("chapterIdBookmark - in library=", old_value)
            debug("chapterIdBookmark - on kobo==in library=", new_value == old_value)

            if value_changed(old_value, new_value):
                id_map_chapteridbookmarked[book_id] = new_value
                book_updated = True
            else:
                book_updated = book_updated or False

        if kobo_percentRead_column_name is not None:
            debug("setting kobo_percentRead=", kobo_percentRead)
            metadata = book.get_user_metadata(kobo_percentRead_column_name, True)
            assert metadata is not None
            current_percentRead = metadata["#value#"]
            debug("percent read - in book=", current_percentRead)

            if value_changed(current_percentRead, kobo_percentRead):
                id_map_percentRead[book_id] = kobo_percentRead
                book_updated = True
            else:
                book_updated = book_updated or False

        if rating_column_name is not None and kobo_rating > 0:
            debug("setting rating_column_name=", rating_column_name)
            if rating_column_name == "rating":
                current_rating = book.rating
                debug("rating - in book=", current_rating)
            else:
                metadata = book.get_user_metadata(rating_column_name, True)
                assert metadata is not None
                current_rating = metadata["#value#"]
            if value_changed(current_rating, kobo_rating):
                id_map_rating[book_id] = kobo_rating
                book_updated = True
            else:
                book_updated = book_updated or False

        if time_spent_reading_column_name is not None:
            metadata = book.get_user_metadata(time_spent_reading_column_name, True)
            assert metadata is not None
            current_time_spent_reading = metadata["#value#"]
            debug(
                "book.get_user_metadata(time_spent_reading_column_name, True)['#value#']=",
                current_time_spent_reading,
            )
            debug("setting mi.time_spent_reading=", time_spent_reading)
            debug(
                "current_time_spent_reading == time_spent_reading=",
                current_time_spent_reading == time_spent_reading,
            )

            if value_changed(current_time_spent_reading, time_spent_reading):
                id_map_time_spent_reading[book_id] = time_spent_reading
                book_updated = True
            else:
                book_updated = book_updated or False

        if rest_of_book_estimate_column_name is not None:
            metadata = book.get_user_metadata(rest_of_book_estimate_column_name, True)
            assert metadata is not None
            current_rest_of_book_estimate = metadata["#value#"]
            debug(
                "book.get_user_metadata(rest_of_book_estimate_column_name , True)['#value#']=",
                current_rest_of_book_estimate,
            )
            debug("setting mi.rest_of_book_estimate=", rest_of_book_estimate)
            debug(
                "current_rest_of_book_estimate == rest_of_book_estimate=",
                current_rest_of_book_estimate == rest_of_book_estimate,
            )

            if value_changed(current_rest_of_book_estimate, rest_of_book_estimate):
                id_map_rest_of_book_estimate[book_id] = rest_of_book_estimate
                book_updated = True
            else:
                book_updated = book_updated or False

        id_map[book_id] = mi

    if kobo_chapteridbookmarked_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (
                kobo_chapteridbookmarked_column_name,
                len(id_map_chapteridbookmarked),
            )
        )
        library_db.new_api.set_field(
            kobo_chapteridbookmarked_column_name, id_map_chapteridbookmarked
        )
    if kobo_percentRead_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (kobo_percentRead_column_name, len(id_map_percentRead))
        )
        library_db.new_api.set_field(kobo_percentRead_column_name, id_map_percentRead)
    if rating_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (rating_column_name, len(id_map_rating))
        )
        library_db.new_api.set_field(rating_column_name, id_map_rating)
    if last_read_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (last_read_column_name, len(id_map_last_read))
        )
        library_db.new_api.set_field(last_read_column_name, id_map_last_read)
    if time_spent_reading_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (time_spent_reading_column_name, len(id_map_time_spent_reading))
        )
        library_db.new_api.set_field(
            time_spent_reading_column_name, id_map_time_spent_reading
        )
    if rest_of_book_estimate_column_name:
        debug(
            "Updating metadata - for column: %s number of changes=%d"
            % (
                rest_of_book_estimate_column_name,
                len(id_map_rest_of_book_estimate),
            )
        )
        library_db.new_api.set_field(
            rest_of_book_estimate_column_name, id_map_rest_of_book_estimate
        )

    debug("Updating GUI - new DB engine")
    gui.iactions["Edit Metadata"].refresh_gui(list(reading_locations))
    debug("finished")

    progressbar.hide()
    gui.status_bar.show_message(
        _("Kobo Utilities")
        + " - "
        + _("Storing reading positions completed - {0} changed.").format(
            len(reading_locations)
        ),
        3000,
    )


def _get_fetch_query_for_firmware_version(
    current_firmware_version: tuple[int, int, int],
) -> FetchQueries | None:
    fetch_queries = None
    for fw_version in sorted(FETCH_QUERIES.keys()):
        if current_firmware_version < fw_version:
            break
        fetch_queries = FETCH_QUERIES[fw_version]

    debug("using fetch_queries:", fetch_queries)
    return fetch_queries


class BookmarkOptionsDialog(PluginDialog):
    def __init__(self, parent: ui.Main, device: KoboDevice):
        super().__init__(
            parent,
            "kobo utilities plugin:bookmark options dialog",
        )
        self.gui = parent
        self.device = device

        library_config = cfg.get_library_config(parent.current_db)
        self.profiles = library_config.profiles
        self.profile_name = (
            device.profile.profileName if device and device.profile else None
        )
        self.initialize_controls()

        options = cfg.plugin_prefs.BookmarkOptions
        if options.storeBookmarks:
            self.store_radiobutton.click()
        else:
            self.restore_radiobutton.click()
        self.status_to_reading_checkbox.setChecked(options.readingStatus)
        self.date_to_now_checkbox.setChecked(options.setDateToNow)
        self.set_rating_checkbox.setChecked(options.rating and device.supports_ratings)

        self.clear_if_unread_checkbox.setChecked(options.clearIfUnread)
        self.store_if_more_recent_checkbox.setChecked(options.storeIfMoreRecent)
        self.do_not_store_if_reopened_checkbox.setChecked(options.doNotStoreIfReopened)
        self.do_not_store_if_reopened_checkbox_clicked(options.doNotStoreIfReopened)
        self.background_checkbox.setChecked(options.backgroundJob)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Store or restore reading positions"),
            "StoreCurrentBookmark",
        )
        layout.addLayout(title_layout)

        options_column_group = QGroupBox(_("Options"), self)
        layout.addWidget(options_column_group)
        options_layout = QGridLayout()
        options_column_group.setLayout(options_layout)

        self.store_radiobutton = QRadioButton(_("Store"), self)
        self.store_radiobutton.setToolTip(
            _("Store the current reading position in the calibre library.")
        )
        options_layout.addWidget(self.store_radiobutton, 1, 0, 1, 1)
        self.store_radiobutton.clicked.connect(self.store_radiobutton_clicked)

        self.store_if_more_recent_checkbox = QCheckBox(_("Only if more recent"), self)
        self.store_if_more_recent_checkbox.setToolTip(
            _(
                "Only store the reading position if the last read timestamp on the device is more recent than in the library."
            )
        )
        options_layout.addWidget(self.store_if_more_recent_checkbox, 2, 0, 1, 1)

        self.do_not_store_if_reopened_checkbox = QCheckBox(
            _("Not if finished in library"), self
        )
        self.do_not_store_if_reopened_checkbox.setToolTip(
            _(
                "Do not store the reading position if the library has the book as finished. This is if the percent read is 100%."
            )
        )
        options_layout.addWidget(self.do_not_store_if_reopened_checkbox, 3, 0, 1, 1)
        self.do_not_store_if_reopened_checkbox.clicked.connect(
            self.do_not_store_if_reopened_checkbox_clicked
        )

        self.clear_if_unread_checkbox = QCheckBox(_("Clear if unread"), self)
        self.clear_if_unread_checkbox.setToolTip(
            _(
                "If the book on the device is shown as unread, clear the reading position stored in the library."
            )
        )
        options_layout.addWidget(self.clear_if_unread_checkbox, 4, 0, 1, 1)

        self.background_checkbox = QCheckBox(_("Run in background"), self)
        self.background_checkbox.setToolTip(_("Do store or restore as background job."))
        options_layout.addWidget(self.background_checkbox, 5, 0, 1, 2)

        self.restore_radiobutton = QRadioButton(_("Restore"), self)
        self.restore_radiobutton.setToolTip(
            _("Copy the current reading position back to the device.")
        )
        options_layout.addWidget(self.restore_radiobutton, 1, 1, 1, 1)
        self.restore_radiobutton.clicked.connect(self.restore_radiobutton_clicked)

        self.status_to_reading_checkbox = QCheckBox(_("Set reading status"), self)
        self.status_to_reading_checkbox.setToolTip(
            _(
                "If this is not set, when the current reading position is on the device, the reading status will not be changes. If the percent read is 100%, the book will be marked as finished. Otherwise, it will be in progress."
            )
        )
        options_layout.addWidget(self.status_to_reading_checkbox, 2, 1, 1, 1)

        self.date_to_now_checkbox = QCheckBox(_("Set date to now"), self)
        self.date_to_now_checkbox.setToolTip(
            _(
                'Setting the date to now will put the book at the top of the "Recent reads" list.'
            )
        )
        options_layout.addWidget(self.date_to_now_checkbox, 3, 1, 1, 1)

        self.set_rating_checkbox = QCheckBox(_("Update rating"), self)
        self.set_rating_checkbox.setToolTip(
            _(
                "Set the book rating on the device. If the current rating in the library is zero, the rating on the device will be reset."
            )
        )
        options_layout.addWidget(self.set_rating_checkbox, 4, 1, 1, 1)

        profiles_label = QLabel(_("Profile"), self)
        options_layout.addWidget(profiles_label, 6, 0, 1, 1)
        self.select_profile_combo = cfg.ProfileComboBox(
            self, self.profiles, self.profile_name
        )
        self.select_profile_combo.setMinimumSize(150, 20)
        options_layout.addWidget(self.select_profile_combo, 6, 1, 1, 1)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self):
        profile_name = str(self.select_profile_combo.currentText()).strip()
        msg = cfg.validate_profile(profile_name, self.gui, self.device)
        if msg is not None:
            error_dialog(
                self, "Invalid profile", msg, show=True, show_copy_button=False
            )
            return
        self.profile_name = profile_name
        with cfg.plugin_prefs.BookmarkOptions as options:
            options.storeBookmarks = self.store_radiobutton.isChecked()
            options.readingStatus = self.status_to_reading_checkbox.isChecked()
            options.setDateToNow = self.date_to_now_checkbox.isChecked()
            options.rating = self.set_rating_checkbox.isChecked()
            options.clearIfUnread = self.clear_if_unread_checkbox.isChecked()
            options.storeIfMoreRecent = self.store_if_more_recent_checkbox.isChecked()
            options.doNotStoreIfReopened = (
                self.do_not_store_if_reopened_checkbox.isChecked()
            )
            options.backgroundJob = self.background_checkbox.isChecked()
            if options.doNotStoreIfReopened:
                options.clearIfUnread = False
        self.accept()

    def do_not_store_if_reopened_checkbox_clicked(self, checked: bool):
        self.clear_if_unread_checkbox.setEnabled(not checked)

    def restore_radiobutton_clicked(self, checked: bool):
        self.status_to_reading_checkbox.setEnabled(checked)
        self.date_to_now_checkbox.setEnabled(checked)
        device = self.device
        has_rating_column = cfg.get_column_names(self.gui, device).rating != ""
        self.set_rating_checkbox.setEnabled(
            checked and has_rating_column and device.supports_ratings
        )
        self.clear_if_unread_checkbox.setEnabled(not checked)
        self.store_if_more_recent_checkbox.setEnabled(not checked)
        self.do_not_store_if_reopened_checkbox.setEnabled(not checked)
        self.background_checkbox.setEnabled(not checked)

    def store_radiobutton_clicked(self, checked: bool):
        self.status_to_reading_checkbox.setEnabled(not checked)
        self.date_to_now_checkbox.setEnabled(not checked)
        self.set_rating_checkbox.setEnabled(not checked)
        self.clear_if_unread_checkbox.setEnabled(checked)
        self.store_if_more_recent_checkbox.setEnabled(checked)
        self.do_not_store_if_reopened_checkbox.setEnabled(checked)
        self.background_checkbox.setEnabled(checked)


class ReadLocationsProgressDialog(QProgressDialog):
    def __init__(
        self,
        gui: ui.Main,
        device: KoboDevice,
        dispatcher: Dispatcher,
        options: ReadLocationsJobOptions,
        db: LibraryDatabase | None,
    ):
        QProgressDialog.__init__(self, "", "", 0, 0, gui)
        debug("init")
        self.setMinimumWidth(500)
        self.books = []
        self.options = options
        self.db = db
        self.gui = gui
        self.dispatcher = dispatcher
        self.device = device
        self.i = 0
        self.books_to_scan = []
        self.profileName = self.options.profile_name
        self.setWindowTitle(_("Queueing books for storing reading position"))
        QTimer.singleShot(0, self.do_books)
        self.exec()

    def do_books(self):
        debug("Start")

        library_db = self.db
        assert library_db is not None

        custom_columns = cfg.get_column_names(self.gui, self.device)
        self.options.custom_columns = custom_columns
        kobo_chapteridbookmarked_column = custom_columns.current_location
        kobo_percentRead_column = custom_columns.percent_read
        rating_column = custom_columns.rating
        last_read_column = custom_columns.last_read
        time_spent_reading_column = custom_columns.time_spent_reading
        rest_of_book_estimate_column = custom_columns.rest_of_book_estimate

        debug("kobo_percentRead_column='%s'" % kobo_percentRead_column)
        self.setLabelText(_("Preparing the list of books ..."))
        self.setValue(1)
        search_condition = ""
        if self.options.bookmark_options.doNotStoreIfReopened:
            search_condition = f"and ({kobo_percentRead_column}:false or {kobo_percentRead_column}:<100)"
        if self.options.allOnDevice:
            search_condition = f"ondevice:True {search_condition}"
            debug("search_condition=", search_condition)
            onDeviceIds = set(
                library_db.search_getting_ids(  # pyright: ignore[reportAttributeAccessIssue]
                    search_condition,
                    None,
                    sort_results=False,
                    use_virtual_library=False,
                )
            )
        else:
            onDeviceIds = utils.get_selected_ids(self.gui)

        self.books = utils.convert_calibre_ids_to_books(library_db, onDeviceIds)
        self.setRange(0, len(self.books))
        device = self.device
        assert device is not None
        for book in self.books:
            self.i += 1
            device_book_paths = utils.get_device_paths_from_id(
                cast("int", book.calibre_id), self.gui
            )
            book.contentIDs = [
                utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
                for path in device_book_paths
            ]
            if len(book.contentIDs):
                title = book.title
                self.setLabelText(_("Queueing {}").format(title))
                authors = authors_to_string(book.authors)
                current_chapterid = None
                current_percentRead = None
                current_rating = None
                current_last_read = None
                current_time_spent_reading = None
                current_rest_of_book_estimate = None
                if kobo_chapteridbookmarked_column:
                    metadata = book.get_user_metadata(
                        kobo_chapteridbookmarked_column, True
                    )
                    assert metadata is not None
                    current_chapterid = metadata["#value#"]
                if kobo_percentRead_column:
                    metadata = book.get_user_metadata(kobo_percentRead_column, True)
                    assert metadata is not None
                    current_percentRead = metadata["#value#"]
                if rating_column:
                    if rating_column == "rating":
                        current_rating = book.rating
                    else:
                        metadata = book.get_user_metadata(rating_column, True)
                        assert metadata is not None
                        current_rating = metadata["#value#"]
                if last_read_column:
                    metadata = book.get_user_metadata(last_read_column, True)
                    assert metadata is not None
                    current_last_read = metadata["#value#"]
                if time_spent_reading_column:
                    metadata = book.get_user_metadata(time_spent_reading_column, True)
                    assert metadata is not None
                    current_time_spent_reading = metadata["#value#"]
                if rest_of_book_estimate_column:
                    metadata = book.get_user_metadata(
                        rest_of_book_estimate_column, True
                    )
                    assert metadata is not None
                    current_rest_of_book_estimate = metadata["#value#"]

                self.books_to_scan.append(
                    (
                        book.calibre_id,
                        book.contentIDs,
                        title,
                        authors,
                        current_chapterid,
                        current_percentRead,
                        current_rating,
                        current_last_read,
                        current_time_spent_reading,
                        current_rest_of_book_estimate,
                    )
                )
            self.setValue(self.i)

        debug("Finish")
        self.hide()

        # Queue a job to process these ePub books
        _store_queue_job(
            self.dispatcher,
            self.device,
            self.gui,
            self.options,
            self.books_to_scan,
        )


class ShowReadingPositionChangesDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        reading_locations: dict[int, dict[str, Any]],
        device: KoboDevice,
        db: LibraryDatabase,
        profileName: str | None,
        goodreads_sync_installed: bool = False,
    ):
        super().__init__(
            parent,
            "kobo utilities plugin:show reading position changes dialog",
        )
        self.gui = parent
        self.reading_locations = reading_locations
        self.device = device
        self.blockSignals(True)
        self.db = db

        self.profileName = (
            device.profile.profileName
            if not profileName and device.profile is not None
            else profileName
        )
        self.deviceName = cfg.get_device_name(device.version_info.serial_no)
        options = cfg.get_library_config(parent.current_db).readingPositionChangesStore

        self.initialize_controls()

        # Display the books in the table
        self.blockSignals(False)
        self.reading_locations_table.populate_table(self.reading_locations)

        self.select_books_checkbox.setChecked(options.selectBooksInLibrary)
        update_goodreads_progress = options.updeateGoodreadsProgress
        self.update_goodreads_progress_checkbox.setChecked(update_goodreads_progress)
        if goodreads_sync_installed:
            self.update_goodreads_progress_checkbox_clicked(update_goodreads_progress)
        else:
            self.update_goodreads_progress_checkbox.setEnabled(False)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(_("Show reading position changes"))
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/manage_series.png",
            _("Show reading position changes"),
            "ShowReadingPositionChanges",
        )
        layout.addLayout(title_layout)

        # Main series table layout
        table_layout = QGridLayout()
        layout.addLayout(table_layout)

        table_layout.addWidget(
            QLabel(_("Profile: {0}").format(self.profileName)), 0, 0, 1, 1
        )
        table_layout.addWidget(
            QLabel(_("Device: {0}").format(self.deviceName)), 0, 2, 1, 1
        )

        self.reading_locations_table = ShowReadingPositionChangesTableWidget(
            self, self.device, self.db
        )
        table_layout.addWidget(self.reading_locations_table, 1, 0, 1, 4)

        self.select_books_checkbox = QCheckBox(_("Select updated books in library"))
        table_layout.addWidget(self.select_books_checkbox, 2, 0, 1, 2)

        self.update_goodreads_progress_checkbox = QCheckBox(
            _("Update Goodread reading progress")
        )
        self.update_goodreads_progress_checkbox.clicked.connect(
            self.update_goodreads_progress_checkbox_clicked
        )
        table_layout.addWidget(self.update_goodreads_progress_checkbox, 2, 1, 1, 2)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._ok_clicked)
        button_box.rejected.connect(self.reject)
        select_all_button = button_box.addButton(
            _("Select all"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert select_all_button is not None
        select_all_button.clicked.connect(self._select_all_clicked)
        clear_all_button = button_box.addButton(
            _("Clear all"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert clear_all_button is not None
        clear_all_button.clicked.connect(self._clear_all_clicked)

        layout.addWidget(button_box)

    def _ok_clicked(self):
        library_config = cfg.get_library_config(self.gui.current_db)
        library_config.readingPositionChangesStore.selectBooksInLibrary = (
            self.select_books_checkbox.isChecked()
        )
        library_config.readingPositionChangesStore.updeateGoodreadsProgress = (
            self.update_goodreads_progress_checkbox.isChecked()
        )
        cfg.set_library_config(self.gui.current_db, library_config)

        for i in range(len(self.reading_locations)):
            self.reading_locations_table.selectRow(i)
            item = self.reading_locations_table.item(i, 0)
            assert item is not None
            enabled = item.checkState() == Qt.CheckState.Checked
            debug("row=%d, enabled=%s" % (i, enabled))
            if not enabled:
                item = self.reading_locations_table.item(i, 7)
                assert item is not None
                book_id = item.data(Qt.ItemDataRole.DisplayRole)
                debug("row=%d, book_id=%s" % (i, book_id))
                del self.reading_locations[book_id]
        self.accept()
        return

    def _select_all_clicked(self):
        self.reading_locations_table.toggle_checkmarks(Qt.CheckState.Checked)

    def _clear_all_clicked(self):
        self.reading_locations_table.toggle_checkmarks(Qt.CheckState.Unchecked)

    def update_goodreads_progress_checkbox_clicked(self, checked: bool):
        self.select_books_checkbox.setEnabled(not checked)


class ShowReadingPositionChangesTableWidget(QTableWidget):
    def __init__(
        self,
        parent: ShowReadingPositionChangesDialog,
        device: KoboDevice,
        db: LibraryDatabase,
    ):
        QTableWidget.__init__(self, parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.db = db

        custom_columns = cfg.get_column_names(parent.gui, device)
        self.kobo_percentRead_column = custom_columns.percent_read
        self.last_read_column = custom_columns.last_read

    def populate_table(self, reading_positions: dict[int, dict[str, Any]]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(reading_positions))
        header_labels = [
            "",
            _("Title"),
            _("Authors(s)"),
            _("Current %"),
            _("New %"),
            _("Current date"),
            _("New date"),
            _("Book ID"),
        ]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.setDefaultSectionSize(24)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(True)

        debug("reading_positions=", reading_positions)
        for row, (book_id, reading_position) in enumerate(reading_positions.items()):
            self.populate_table_row(row, book_id, reading_position)

        self.resizeColumnToContents(0)
        self.resizeColumnToContents(1)
        self.setMinimumColumnWidth(1, 150)
        self.setColumnWidth(2, 100)
        self.resizeColumnToContents(3)
        self.resizeColumnToContents(4)
        self.resizeColumnToContents(5)
        self.resizeColumnToContents(6)
        self.hideColumn(7)
        self.setSortingEnabled(True)
        self.selectRow(0)
        delegate = DateDelegate(self)
        self.setItemDelegateForColumn(5, delegate)
        self.setItemDelegateForColumn(6, delegate)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(
        self, row: int, book_id: int, reading_position: dict[str, Any]
    ):
        self.blockSignals(True)

        book = self.db.get_metadata(book_id, index_is_id=True, get_cover=False)

        self.setItem(row, 0, CheckableTableWidgetItem(True))

        titleColumn = QTableWidgetItem(reading_position["Title"])
        titleColumn.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, 1, titleColumn)

        authorColumn = AuthorsTableWidgetItem(book.authors, book.author_sort)
        self.setItem(row, 2, authorColumn)

        current_percentRead = None
        if self.kobo_percentRead_column:
            metadata = book.get_user_metadata(self.kobo_percentRead_column, True)
            assert metadata is not None
            current_percentRead = metadata["#value#"]
        current_percent = RatingTableWidgetItem(current_percentRead, is_read_only=True)
        current_percent.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.setItem(row, 3, current_percent)

        new_percentRead = 0
        if reading_position["ReadStatus"] == 1:
            new_percentRead = reading_position["___PercentRead"]
        elif reading_position["ReadStatus"] == 2:
            new_percentRead = 100
        new_percent = RatingTableWidgetItem(new_percentRead, is_read_only=True)
        new_percent.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.setItem(row, 4, new_percent)

        current_last_read = None
        if self.last_read_column:
            metadata = book.get_user_metadata(self.last_read_column, True)
            assert metadata is not None
            current_last_read = metadata["#value#"]
        if current_last_read:
            self.setItem(
                row,
                5,
                DateTableWidgetItem(
                    current_last_read, is_read_only=True, default_to_today=False
                ),
            )
        if reading_position["DateLastRead"] is not None:
            self.setItem(
                row,
                6,
                DateTableWidgetItem(
                    utils.convert_kobo_date(reading_position["DateLastRead"]),
                    is_read_only=True,
                    default_to_today=False,
                ),
            )
        book_idColumn = RatingTableWidgetItem(book_id)
        self.setItem(row, 7, book_idColumn)
        self.blockSignals(False)

    def toggle_checkmarks(self, select: Qt.CheckState):
        for i in range(self.rowCount()):
            item = self.item(i, 0)
            assert item is not None
            item.setCheckState(select)


####################
# Jobs
# ##################


def do_read_locations(
    books_to_scan: list[
        tuple[
            int,
            list[str],
            str,
            list[str],
            str | None,
            int | None,
            int | None,
            dt.datetime | None,
            int | None,
            int | None,
        ]
    ],
    options: ReadLocationsJobOptions,
    cpus: int,
    notification: Callable[[float, str], Any] = lambda _x, y: y,
) -> tuple[dict[int, dict[str, Any]], ReadLocationsJobOptions]:
    """
    Master job to do read the current reading locations from the device DB
    """
    debug("start")
    server = Server(pool_size=cpus)

    debug("options=%s" % (options))
    # Queue all the jobs

    args = [
        do_read_locations_all.__module__,
        do_read_locations_all.__name__,
        (books_to_scan, pickle.dumps(options)),
    ]
    debug("len(books_to_scan)=%d" % (len(books_to_scan)))
    job: ParallelJob = ParallelJob("arbitrary", "Read locations", done=None, args=args)
    server.add_job(job)

    # This server is an arbitrary_n job, so there is a notifier available.
    # Set the % complete to a small number to avoid the 'unavailable' indicator
    notification(0.01, "Reading device database")

    # dequeue the job results as they arrive, saving the results
    total = 1
    count = 0
    new_locations = {}
    while True:
        job = server.changed_jobs_queue.get()
        # A job can 'change' when it is not finished, for example if it
        # produces a notification. Ignore these.
        job.update()
        if not job.is_finished:
            debug("Job not finished")
            continue
        # A job really finished. Get the information.
        new_locations = cast("dict[int, dict[str, Any]]", job.result)
        count += 1
        notification(float(count) / total, "Storing locations")
        number_locations = len(new_locations) if new_locations else 0
        debug("count=%d" % number_locations)
        debug(job.details)
        if count >= total:
            # All done!
            break

    server.close()
    debug("finished")
    # return the map as the job result
    return new_locations, options


def do_read_locations_all(
    books: list[
        tuple[
            int,
            list[str],
            str,
            list[str],
            str | None,
            int | None,
            int | None,
            dt.datetime | None,
            int | None,
            int | None,
        ]
    ],
    options: bytes,
) -> dict[int, dict[str, Any]]:
    """
    Child job, to read location for all the books
    """
    return _read_locations(books, pickle.loads(options))  # noqa: S301


def _read_locations(
    books: list[
        tuple[
            int,
            list[str],
            str,
            list[str],
            str | None,
            int | None,
            int | None,
            dt.datetime | None,
            int | None,
            int | None,
        ]
    ],
    options: ReadLocationsJobOptions,
) -> dict[int, dict[str, Any]]:
    debug("start")
    count_books = 0
    new_locations = {}
    clear_if_unread = options.bookmark_options.clearIfUnread
    store_if_more_recent = options.bookmark_options.storeIfMoreRecent
    do_not_store_if_reopened = options.bookmark_options.doNotStoreIfReopened
    epub_location_like_kepub = options.epub_location_like_kepub
    kepub_fetch_query = options.fetch_queries.kepub
    epub_fetch_query = options.fetch_queries.epub

    kobo_percentRead_column_name = None
    last_read_column_name = None
    if options.custom_columns is not None:
        kobo_percentRead_column_name = options.custom_columns.percent_read
        last_read_column_name = options.custom_columns.last_read

    connection = DeviceDatabaseConnection(
        options.database_path,
        options.device_database_path,
        options.is_db_copied,
        use_row_factory=True,
    )
    cursor = connection.cursor()
    count_books += 1

    debug("about to start book loop")
    for (
        book_id,
        contentIDs,
        title,
        authors,
        current_chapterid,
        current_percentRead,
        current_rating,
        current_last_read,
        current_time_spent_reading,
        current_rest_of_book_estimate,
    ) in books:
        debug("----------- top of loop -----------")
        debug("Current book: %s - %s" % (title, authors))
        debug("contentIds='%s'" % (contentIDs))
        device_status = None
        contentID = None
        for contentID in contentIDs:
            debug("contentId='%s'" % (contentID))
            fetch_values = (contentID,)
            if contentID.endswith(".kepub.epub"):
                fetch_query = kepub_fetch_query
            else:
                fetch_query = epub_fetch_query
            cursor.execute(fetch_query, fetch_values)
            result = None
            try:
                result = next(cursor)
                debug("device_status='%s'" % (device_status))
                debug("result='%s'" % (result))
                if device_status is None:
                    debug("device_status is None")
                    device_status = result
                elif (
                    result["DateLastRead"] is not None
                    and device_status["DateLastRead"] is None
                ):
                    debug(
                        "result['DateLastRead'] is not None - result['DateLastRead']='%s'"
                        % result["DateLastRead"]
                    )
                    debug("device_status['DateLastRead'] is None")
                    device_status = result
                elif (
                    result["DateLastRead"] is not None
                    and device_status["DateLastRead"] is not None
                    and (result["DateLastRead"] > device_status["DateLastRead"])
                ):
                    debug(
                        "result['DateLastRead'] > device_status['DateLastRead']=%s"
                        % result["DateLastRead"]
                        > device_status["DateLastRead"]
                    )
                    device_status = result
            except TypeError:
                debug("TypeError for: contentID='%s'" % (contentID))
                debug("device_status='%s'" % (device_status))
                debug("database result='%s'" % (result))
                raise
            except StopIteration:
                pass

        if not device_status:
            continue

        new_last_read = None
        if device_status["DateLastRead"]:
            new_last_read = utils.convert_kobo_date(device_status["DateLastRead"])

        if last_read_column_name is not None and store_if_more_recent:
            debug("setting mi.last_read=", new_last_read)
            if current_last_read is not None and new_last_read is not None:
                debug(
                    "store_if_more_recent - current_last_read < new_last_read=",
                    current_last_read < new_last_read,
                )
                if current_last_read >= new_last_read:
                    continue
            elif current_last_read is not None and new_last_read is None:
                continue

        if kobo_percentRead_column_name is not None and do_not_store_if_reopened:
            debug(
                "do_not_store_if_reopened - current_percentRead=", current_percentRead
            )
            if current_percentRead is not None and current_percentRead >= 100:
                continue

        debug("finished reading database for book - device_status=", device_status)
        kobo_chapteridbookmarked = None
        kobo_adobe_location = None
        if device_status["MimeType"] == MIMETYPE_KOBO or epub_location_like_kepub:
            kobo_chapteridbookmarked = device_status["ChapterIDBookmarked"]
            kobo_adobe_location = None
        elif contentID is not None:
            kobo_chapteridbookmarked = (
                device_status["ChapterIDBookmarked"][len(contentID) + 1 :]
                if device_status["ChapterIDBookmarked"]
                else None
            )
            kobo_adobe_location = device_status["adobe_location"]
        if kobo_chapteridbookmarked and kobo_adobe_location:
            new_chapterid = (
                kobo_chapteridbookmarked + BOOKMARK_SEPARATOR + kobo_adobe_location
            )
        elif kobo_chapteridbookmarked:
            new_chapterid = kobo_chapteridbookmarked
        else:
            new_chapterid = None

        new_kobo_percentRead = None
        if device_status["ReadStatus"] == 1:
            new_kobo_percentRead = device_status["___PercentRead"]
        elif device_status["ReadStatus"] == 2:
            new_kobo_percentRead = 100

        new_kobo_rating = device_status["Rating"] * 2 if device_status["Rating"] else 0

        if device_status["TimeSpentReading"]:
            new_time_spent_reading = device_status["TimeSpentReading"]
        else:
            new_time_spent_reading = None

        if device_status["RestOfBookEstimate"]:
            new_rest_of_book_estimate = device_status["RestOfBookEstimate"]
        else:
            new_rest_of_book_estimate = None

        reading_position_changed = False
        if device_status["ReadStatus"] == 0 and clear_if_unread:
            reading_position_changed = True
            new_chapterid = None
            new_kobo_percentRead = 0
            new_last_read = None
            new_time_spent_reading = None
            new_rest_of_book_estimate = None
        elif device_status["ReadStatus"] > 0:
            try:
                debug(
                    "Start of checks for current_last_read - reading_position_changed='%s'"
                    % reading_position_changed
                )
                debug("current_last_read='%s'" % current_last_read)
                debug("new_last_read    ='%s'" % new_last_read)
                debug(
                    "current_last_read != new_last_read='%s'"
                    % (current_last_read != new_last_read)
                )
            except Exception:
                debug("Exception raised when logging details of last read. Ignoring.")
            reading_position_changed = reading_position_changed or (
                current_last_read != new_last_read
            )
            debug(
                "After checking current_last_read - reading_position_changed='%s'"
                % reading_position_changed
            )
            if store_if_more_recent:
                if current_last_read is not None and new_last_read is not None:
                    debug(
                        "store_if_more_recent - current_last_read < new_last_read=",
                        current_last_read < new_last_read,
                    )
                    if current_last_read >= new_last_read:
                        debug(
                            "store_if_more_recent - new timestamp not more recent than current timestamp. Do not store."
                        )
                        break
                    reading_position_changed = reading_position_changed and (
                        current_last_read < new_last_read
                    )
                elif new_last_read is not None:
                    reading_position_changed = True

            try:
                debug("current_percentRead ='%s'" % current_percentRead)
                debug("new_kobo_percentRead='%s'" % new_kobo_percentRead)
                debug(
                    "current_percentRead != new_kobo_percentRead='%s'"
                    % (current_percentRead != new_kobo_percentRead)
                )
            except Exception:
                debug(
                    "Exception raised when logging details of percent read. Ignoring."
                )
            debug(
                "After checking percent read - reading_position_changed=",
                reading_position_changed,
            )
            if do_not_store_if_reopened:
                debug(
                    "do_not_store_if_reopened - current_percentRead=",
                    current_percentRead,
                )
                if current_percentRead is not None and current_percentRead >= 100:
                    debug("do_not_store_if_reopened - Already finished. Do not store.")
                    break
            reading_position_changed = (
                reading_position_changed or current_percentRead != new_kobo_percentRead
            )

            try:
                debug("current_chapterid ='%s'" % current_chapterid)
                debug("new_chapterid='%s'" % new_chapterid)
                debug(
                    "current_chapterid != new_chapterid='%s'"
                    % (current_chapterid != new_chapterid)
                )
            except Exception:
                debug(
                    "Exception raised when logging details of percent read. Ignoring."
                )
            reading_position_changed = reading_position_changed or utils.value_changed(
                current_chapterid, new_chapterid
            )
            debug(
                "After checking location - reading_position_changed=",
                reading_position_changed,
            )

            debug(
                "current_rating=%s, new_kobo_rating=%s"
                % (current_rating, new_kobo_rating)
            )
            debug(
                "current_rating != new_kobo_rating=", current_rating != new_kobo_rating
            )
            debug(
                "current_rating != new_kobo_rating and not (current_rating is None and new_kobo_rating == 0)=",
                current_rating != new_kobo_rating
                and not (current_rating is None and new_kobo_rating == 0),
            )
            debug(
                "current_rating != new_kobo_rating and new_kobo_rating > 0=",
                current_rating != new_kobo_rating and new_kobo_rating > 0,
            )
            reading_position_changed = reading_position_changed or (
                current_rating != new_kobo_rating
                and not (current_rating is None and new_kobo_rating == 0)
            )
            reading_position_changed = reading_position_changed or (
                current_rating != new_kobo_rating and new_kobo_rating > 0
            )

            debug(
                "current_time_spent_reading=%s, new_time_spent_reading=%s"
                % (current_time_spent_reading, new_time_spent_reading)
            )
            debug(
                "current_time_spent_reading != new_time_spent_reading=",
                current_time_spent_reading != new_time_spent_reading,
            )
            reading_position_changed = reading_position_changed or utils.value_changed(
                current_time_spent_reading, new_time_spent_reading
            )

            debug(
                "current_rest_of_book_estimate=%s, new_rest_of_book_estimate=%s"
                % (current_rest_of_book_estimate, new_rest_of_book_estimate)
            )
            debug(
                "current_rest_of_book_estimate != new_rest_of_book_estimate=",
                current_rest_of_book_estimate != new_rest_of_book_estimate,
            )
            reading_position_changed = reading_position_changed or utils.value_changed(
                current_rest_of_book_estimate, new_rest_of_book_estimate
            )

        if reading_position_changed:
            debug("position changed for: %s - %s" % (title, authors))
            new_locations[book_id] = device_status

    debug("finished book loop")

    debug("finished")
    return new_locations
