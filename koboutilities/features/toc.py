from __future__ import annotations

import os
import re
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generator, cast

from calibre.devices.kobo.driver import KOBOTOUCH
from calibre.ebooks.metadata import authors_to_string
from calibre.ebooks.oeb.polish.container import EpubContainer
from calibre.ebooks.oeb.polish.errors import DRMError
from calibre.gui2 import question_dialog, ui
from calibre.gui2.dialogs.confirm_delete import confirm
from calibre.utils.logging import default_log
from qt.core import (
    QAbstractItemView,
    QDialogButtonBox,
    QHBoxLayout,
    Qt,
    QTableWidget,
    QVBoxLayout,
)

from .. import utils
from ..constants import GUI_NAME
from ..dialogs import (
    CheckableTableWidgetItem,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    ReadOnlyTableWidgetItem,
    ReadOnlyTextIconWidgetItem,
)
from ..utils import debug

if TYPE_CHECKING:
    from calibre.db.legacy import LibraryDatabase
    from calibre.ebooks.oeb.polish.toc import TOC
    from qt.core import QIcon, QWidget

    from ..config import KoboDevice
    from ..utils import DeviceDatabaseConnection, Dispatcher, LoadResources


def update_book_toc_on_device(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
):
    """Compare the ToC between calibre and the device and update it."""

    del dispatcher, load_resources
    debug("start")

    if not utils.check_device_is_ready(
        device, gui, _("Cannot update the ToC of books on the device")
    ):
        return

    if len(gui.library_view.get_selected_ids()) == 0:
        debug("no books selected")
        return

    db: LibraryDatabase = gui.current_db

    # Use local versions as just need a few details.
    def _convert_calibre_ids_to_books(db: LibraryDatabase, ids: list[int]):
        return [_convert_calibre_id_to_book(db, book_id) for book_id in ids]

    def _convert_calibre_id_to_book(
        db: LibraryDatabase, book_id: int, get_cover: bool = False
    ):
        mi = db.get_metadata(book_id, index_is_id=True, get_cover=get_cover)
        book: dict[str, Any] = {}
        book["good"] = True
        book["calibre_id"] = mi.id
        book["title"] = mi.title
        book["author"] = authors_to_string(mi.authors)
        book["author_sort"] = mi.author_sort
        book["comment"] = ""
        book["url"] = ""
        book["added"] = False
        return book

    book_ids: list[int] = gui.library_view.get_selected_ids()
    books = _convert_calibre_ids_to_books(db, book_ids)
    progressbar = ProgressBar(
        parent=gui, window_title=_("Getting ToC status for books"), on_top=True
    )
    progressbar.set_label(_("Number of books: {0}").format(len(books)))
    progressbar.show_with_maximum(len(books))

    _get_chapter_status(device, gui, db, books, progressbar)

    progressbar.hide()

    d = UpdateBooksToCDialog(gui, books)
    d.exec()
    if d.result() != d.DialogCode.Accepted:
        return

    update_books = d.books_to_update_toc
    debug("len(update_books)=%s" % len(update_books))

    debug("update_books=%d" % len(update_books))
    # only if there's some good ones.
    update_books = list(filter(lambda x: not x["good"], update_books))
    debug("filtered update_books=%d" % len(update_books))
    if len(update_books) > 0:
        update_device_toc_for_books(update_books, device, gui)


def load_ebook(pathtoebook: str) -> EpubContainer:
    debug("creating container")
    try:
        container = EpubContainer(pathtoebook, default_log)
    except DRMError:
        container = None
        raise

    return container


def _read_toc(
    toc: TOC,
    toc_depth: int = 1,
    format_on_device: str = "EPUB",
    container: EpubContainer | None = None,
) -> list[dict[str, Any]]:
    chapters = []
    debug("toc.title=", toc.title)
    debug("toc_depth=", toc_depth)
    debug("parsing ToC")
    for item in toc:
        debug("item.title=", item.title)
        debug("item.depth=", item.depth)
        if item.dest is not None:
            chapter = {}
            chapter["title"] = item.title
            chapter["path"] = item.dest
            if format_on_device == "KEPUB" and container is not None:
                chapter["path"] = container.name_to_href(item.dest, container.opf_name)
            chapter["toc_depth"] = toc_depth
            if item.frag:
                chapter["fragment"] = item.frag
                chapter["path"] = "{0}#{1}".format(chapter["path"], item.frag)
            if format_on_device == "KEPUB":
                chapter["path"] = "{0}-{1}".format(chapter["path"], toc_depth)
            chapter["added"] = False
            chapters.append(chapter)
        chapters += _read_toc(
            item,
            toc_depth + 1,
            format_on_device=format_on_device,
            container=container,
        )

    debug("finished")
    return chapters


def _get_manifest_entries(container: EpubContainer) -> list[dict[str, Any]]:
    debug("start")
    manifest_entries = []
    for spine_name in _get_spine_names(container):
        spine_path = container.name_to_href(spine_name, container.opf_name)
        file_size = container.filesize(spine_name)
        manifest_entries.append(
            {"path": spine_path, "file_size": file_size, "name": spine_name}
        )
    debug("manifest_entries=", manifest_entries)
    return manifest_entries


# Copied from container#spine_iter(), but returns all items in order
# instead of returning non-linear items at the end
def _get_spine_names(container: EpubContainer) -> Generator[str | None, None, None]:
    manifest_id_map = container.manifest_id_map
    for item in container.opf_xpath("//opf:spine/opf:itemref[@idref]"):
        idref = item.get("idref")
        name = manifest_id_map.get(idref, None)
        path = container.name_path_map.get(name, None)
        if path:
            yield name


def _get_chapter_list(
    book: dict[str, Any],
    pathtoebook: str,
    book_location: str,
    format_on_device: str = "EPUB",
):
    debug("for %s" % book_location)
    from calibre.ebooks.oeb.polish.toc import get_toc

    container = load_ebook(pathtoebook)
    debug("container.opf_dir='%s'" % container.opf_dir)
    debug("container.opf_name='%s'" % container.opf_name)
    book[book_location + "_opf_name"] = container.opf_name
    book[book_location + "_opf_dir"] = container.opf_dir
    last_slash_index = book[book_location + "_opf_name"].rfind("/")
    book[book_location + "_opf_dir"] = (
        book[book_location + "_opf_name"][:last_slash_index]
        if last_slash_index >= 0
        else ""
    )
    debug("book[book_location + '_opf_dir']='%s'" % book[book_location + "_opf_dir"])
    toc = get_toc(container)
    debug("toc=", toc)

    chapters = _read_toc(toc, format_on_device=format_on_device, container=container)

    # Remove duplicate content paths while preserving order.
    # This is necessary to avoid DB errors about unique primary keys,
    # and there is no point in having two ToC entries point to the same location anyway.
    # See https://github.com/janlarres/kobo-utilities/issues/8
    # This also prevents the dialog from showing the need to update the ToC
    # when it is in fact up to date, but the number of chapters found here
    # is different from the chapters in the DB because the former contains duplicate paths.
    # This method keeps the last one of the duplicate entries,
    # which is what the Kobo firmware seems to be doing as well.
    chapters = list({chapter["path"]: chapter for chapter in chapters}.values())

    book[book_location + "_chapters"] = chapters
    debug("chapters=", book[book_location + "_chapters"])
    book[book_location + "_manifest"] = _get_manifest_entries(container)
    book[book_location + "_container"] = container
    return


def _get_chapter_status(
    device: KoboDevice,
    gui: ui.Main,
    db: LibraryDatabase,
    books: list[dict[str, Any]],
    progressbar: ProgressBar,
):
    debug(f"Starting check of chapter status for {len(books)} books")
    assert device is not None
    connection = utils.device_database_connection(device, use_row_factory=True)
    i = 0
    debug(
        "device format_map='{0}".format(
            device.driver.settings().format_map  # type: ignore[reportAttributeAccessIssue]
        )
    )
    for book in books:
        progressbar.increment()
        debug(f"Handling book: {book}")
        debug(
            "Getting chapters for book number {0}, title={1}, author={2}".format(
                i, book["title"], book["author"]
            )
        )
        book["library_chapters"] = []
        book["kobo_chapters"] = []
        book["kobo_database_chapters"] = []
        book["kobo_format_status"] = False
        book["kobo_database_status"] = False
        book["can_update_toc"] = False

        book_id = book["calibre_id"]

        debug("Finding book on device...")
        device_book_path = utils.get_device_path_from_id(book_id, gui)
        if device_book_path is None:
            book["comment"] = _("eBook is not on Kobo eReader")
            book["good"] = False
            book["icon"] = "window-close.png"
            book["can_update_toc"] = False
            continue
        extension = os.path.splitext(device_book_path)[1]
        ContentType = (
            device.driver.get_content_type_from_extension(extension)
            if extension != ""
            else device.driver.get_content_type_from_path(device_book_path)
        )
        book["ContentID"] = device.driver.contentid_from_path(
            device_book_path, ContentType
        )
        if ".kepub.epub" in book["ContentID"]:
            book["kobo_format"] = "KEPUB"
        elif ".epub" in book["ContentID"]:
            book["kobo_format"] = "EPUB"
        else:
            book["kobo_format"] = extension[1:].upper()
            book["comment"] = _("eBook on Kobo eReader is not supported format")
            book["good"] = True
            book["icon"] = "window-close.png"
            book["can_update_toc"] = False
            book["kobo_format_status"] = True
            continue

        debug("Checking for book in library...")
        if db.has_format(book_id, book["kobo_format"], index_is_id=True):
            book["library_format"] = book["kobo_format"]
        elif (
            book["kobo_format"] == "KEPUB"
            and "EPUB".lower() in device.driver.settings().format_map  # type: ignore[reportAttributeAccessIssue]
            and db.has_format(book_id, "EPUB", index_is_id=True)
        ):
            book["library_format"] = "EPUB"
        else:
            book["comment"] = _(
                "No suitable format in library for book. The format of the device is {0}"
            ).format(book["kobo_format"])
            book["good"] = False
            continue

        debug("Getting path to book in library...")
        pathtoebook = db.format_abspath(
            book_id, book["library_format"], index_is_id=True
        )
        assert isinstance(pathtoebook, str)
        debug("Getting chapters from library...")
        try:
            _get_chapter_list(
                book,
                pathtoebook,
                "library",
                format_on_device=book["kobo_format"],
            )
        except DRMError:
            book["comment"] = _("eBook in library has DRM")
            book["good"] = False
            book["icon"] = "window-close.png"
            continue

        debug("Getting chapters from book on device...")
        try:
            _get_chapter_list(
                book,
                device_book_path,
                "kobo",
                format_on_device=book["kobo_format"],
            )
        except DRMError:
            book["comment"] = _("eBook on Kobo eReader has DRM")
            book["good"] = False
            book["icon"] = "window-close.png"
            continue

        debug("Getting chapters from device database...")
        if book["kobo_format"] == "KEPUB":
            book["kobo_database_chapters"] = _get_database_chapters(
                connection, book["ContentID"], book["kobo_format"], 899
            )
            debug("book['kobo_database_chapters']=", book["kobo_database_chapters"])
            book["kobo_database_manifest"] = _get_database_chapters(
                connection, book["ContentID"], book["kobo_format"], 9
            )
            debug("book['kobo_database_manifest']=", book["kobo_database_manifest"])
        else:
            book["kobo_database_chapters"] = _get_database_chapters(
                connection, book["ContentID"], book["kobo_format"], 9
            )

        koboDatabaseReadingLocation = _get_database_current_chapter(
            book["ContentID"], device, connection
        )
        if (
            koboDatabaseReadingLocation is not None
            and len(koboDatabaseReadingLocation) > 0
        ):
            book["koboDatabaseReadingLocation"] = koboDatabaseReadingLocation
            if (
                isinstance(device.driver, KOBOTOUCH)
                and (
                    device.driver.fwversion < device.driver.min_fwversion_epub_location
                )  # type: ignore[reportOperatorIssue]
            ):
                reading_location_match = re.match(
                    r"\((\d+)\)(.*)\#?.*", koboDatabaseReadingLocation
                )
                assert reading_location_match is not None
                reading_location_volumeIndex, reading_location_file = (
                    reading_location_match.groups()
                )
                reading_location_volumeIndex = int(reading_location_volumeIndex)
                try:
                    debug(
                        "reading_location_volumeIndex =%d, reading_location_file='%s'"
                        % (reading_location_volumeIndex, reading_location_file)
                    )
                    debug(
                        "chapter location='%s'"
                        % (
                            book["kobo_database_chapters"][
                                reading_location_volumeIndex
                            ]["path"],
                        )
                    )
                except Exception:
                    debug("exception logging reading location details.")
                new_toc_readingposition_index = _get_readingposition_index(
                    book, koboDatabaseReadingLocation
                )
                if new_toc_readingposition_index is not None:
                    try:
                        real_path, chapter_position = book["kobo_database_chapters"][
                            reading_location_volumeIndex
                        ]["path"].split("#")
                        debug("chapter_location='%s'" % (chapter_position,))
                        book["kobo_database_chapters"][reading_location_volumeIndex][
                            "path"
                        ] = real_path
                        new_chapter_position = "{0}#{1}".format(
                            book["library_chapters"][new_toc_readingposition_index][
                                "path"
                            ],
                            chapter_position,
                        )
                        book["library_chapters"][new_toc_readingposition_index][
                            "chapter_position"
                        ] = new_chapter_position
                        book["readingposition_index"] = new_toc_readingposition_index
                        debug("new chapter_location='%s'" % (new_chapter_position,))
                    except Exception:
                        debug("current chapter has not location. Not setting it.")
        debug("len(book['library_chapters']) =", len(book["library_chapters"]))
        debug("len(book['kobo_chapters']) =", len(book["kobo_chapters"]))
        debug(
            "len(book['kobo_database_chapters']) =",
            len(book["kobo_database_chapters"]),
        )
        if len(book["library_chapters"]) == len(book["kobo_database_chapters"]):
            debug("ToC lengths the same in library and database.")
            book["good"] = True
            book["icon"] = "ok.png"
            book["comment"] = "Chapters match in all places"

        if len(book["library_chapters"]) != len(book["kobo_chapters"]):
            debug("ToC lengths different between library and device.")
            book["kobo_format_status"] = False
            book["comment"] = _("Book needs to be updated on Kobo eReader")
            book["icon"] = "toc.png"
        else:
            book["kobo_format_status"] = _compare_toc_entries(
                book, book_format1="library", book_format2="kobo"
            )
            if book["kobo_format"] == "KEPUB":
                book["kobo_format_status"] = book[
                    "kobo_format_status"
                ] and _compare_manifest_entries(
                    book, book_format1="library", book_format2="kobo"
                )
            if book["kobo_format_status"]:
                book["comment"] = (
                    "Chapters in the book on the device do not match the library"
                )
        book["good"] = book["good"] and book["kobo_format_status"]

        if len(book["kobo_database_chapters"]) == 0:
            debug("No chapters in database for book.")
            book["can_update_toc"] = False
            book["kobo_database_status"] = False
            book["comment"] = "Book needs to be imported on the device"
            book["icon"] = "window-close.png"
            continue
        if len(book["kobo_chapters"]) != len(book["kobo_database_chapters"]):
            debug("ToC lengths different between book on device and the database.")
            book["kobo_database_status"] = False
            book["comment"] = "Chapters need to be updated in Kobo eReader database"
            book["icon"] = "toc.png"
            book["can_update_toc"] = True
        else:
            book["kobo_database_status"] = _compare_toc_entries(
                book, book_format1="kobo", book_format2="kobo_database"
            )
            if book["kobo_format"] == "KEPUB":
                book["kobo_database_status"] = book[
                    "kobo_database_status"
                ] and _compare_manifest_entries(
                    book, book_format1="kobo", book_format2="kobo_database"
                )
            if book["kobo_database_status"]:
                book["comment"] = "Chapters need to be updated in Kobo eReader database"
            book["can_update_toc"] = True
        book["good"] = book["good"] and book["kobo_database_status"]

        if book["good"]:
            book["icon"] = "ok.png"
            book["comment"] = "Chapters match in all places"
        else:
            book["icon"] = "toc.png"
            if not book["kobo_format_status"]:
                book["comment"] = _("Book needs to be updated on Kobo eReader")
            elif not book["kobo_database_status"]:
                book["comment"] = "Chapters need to be updated in Kobo eReader database"

        debug("Finished with book")
        i += 1


def _get_database_chapters(
    connection: DeviceDatabaseConnection,
    koboContentId: str,
    book_format: str = "EPUB",
    contentType: int = 9,
) -> list[dict[str, Any]]:
    chapters = []
    debug(
        "koboContentId='%s', book_format='%s', contentId='%s'"
        % (koboContentId, book_format, contentType)
    )
    chapterQuery = (
        "SELECT ContentID, Title, adobe_location, VolumeIndex, Depth, ChapterIDBookmarked "
        "FROM content "
        "WHERE BookID = ?"
        "AND ContentType = ?"
    )
    cursor = connection.cursor()
    t = (koboContentId, contentType)
    cursor.execute(chapterQuery, t)
    for row in cursor:
        chapter = {}
        debug("chapterContentId=%s" % (row["ContentID"],))
        chapter["chapterContentId"] = row["ContentID"]
        chapter["VolumeIndex"] = row["VolumeIndex"]
        chapter["title"] = row["Title"]
        if book_format == "KEPUB":
            path_separator_index = row["ContentID"].find("!")
            path_separator_index = row["ContentID"].find("!", path_separator_index + 1)
            chapter["path"] = row["ContentID"][path_separator_index + 1 :]
        else:
            chapter["path"] = row["ContentID"][len(koboContentId) + 1 :]
            path_separator_index = chapter["path"].find(")")
            chapter["path"] = chapter["path"][path_separator_index + 1 :]
        chapter["adobe_location"] = row["adobe_location"]
        chapter["ChapterIDBookmarked"] = row["ChapterIDBookmarked"]
        chapter["toc_depth"] = row["Depth"]
        chapter["added"] = True
        debug("chapter= ", chapter)
        chapters.append(chapter)

    chapters.sort(key=lambda x: x["VolumeIndex"])

    return chapters


def _get_database_current_chapter(
    koboContentId: str, device: KoboDevice, connection: DeviceDatabaseConnection
) -> str | None:
    debug("start")
    readingLocationchapterQuery = "SELECT ContentID, ChapterIDBookmarked, ReadStatus FROM content WHERE ContentID = ?"
    cursor = connection.cursor()
    t = (koboContentId,)
    cursor.execute(readingLocationchapterQuery, t)
    try:
        result = next(cursor)
        debug("result='%s'" % (result,))
        if result["ChapterIDBookmarked"] is None:
            reading_location = None
        else:
            reading_location = result["ChapterIDBookmarked"]
            assert device is not None
            if (
                isinstance(device.driver, KOBOTOUCH)
                and (
                    device.driver.fwversion < device.driver.min_fwversion_epub_location
                )  # type: ignore[reportOperatorIssue]
            ):
                reading_location = (
                    reading_location[len(koboContentId) + 1 :]
                    if (result["ReadStatus"] == 1)
                    else None
                )
    except StopIteration:
        debug("no match for contentId='%s'" % (koboContentId,))
        reading_location = None
    debug("reading_location='%s'" % (reading_location,))

    return reading_location


def _get_readingposition_index(book: dict[str, Any], koboDatabaseReadingLocation: str):
    new_toc_readingposition_index = None
    reading_location_match = re.match(
        r"\((\d+)\)(.*)\#?.*", koboDatabaseReadingLocation
    )
    assert reading_location_match is not None
    reading_location_volumeIndex, reading_location_file = (
        reading_location_match.groups()
    )
    reading_location_volumeIndex = int(reading_location_volumeIndex)
    try:
        debug(
            "reading_location_volumeIndex =%d, reading_location_file='%s'"
            % (reading_location_volumeIndex, reading_location_file)
        )
        debug(
            "chapter location='%s'"
            % (book["kobo_database_chapters"][reading_location_volumeIndex]["path"],)
        )
        debug(
            "library file='%s'"
            % (book["library_chapters"][reading_location_volumeIndex]["path"],)
        )
    except Exception as e:
        debug("exception getting reading location details. Exception:", e)
        return None

    for i, library_chapter in enumerate(book["library_chapters"]):
        if library_chapter["path"] == reading_location_file:
            new_toc_readingposition_index = i
            debug("found file='%s', index=%s" % (library_chapter["path"], i))
            break

    return new_toc_readingposition_index


def _compare_toc_entries(
    book: dict[str, Any],
    book_format1: str = "library",
    book_format2: str = "kobo",
):
    debug(
        "book_format1='%s', book_format2: %s, count ToC entries: %d"
        % (book_format1, book_format2, len(book[book_format1 + "_chapters"]))
    )
    for i, chapter_format1 in enumerate(book[book_format1 + "_chapters"]):
        chapter_format1_path = chapter_format1["path"]
        chapter_format2_path = book[book_format2 + "_chapters"][i]["path"]
        if chapter_format1_path != chapter_format2_path:
            debug("path different for chapter index: %d" % i)
            debug("format1=%s, path='%s'" % (book_format1, chapter_format1_path))
            debug("format2=%s, path='%s'" % (book_format2, chapter_format2_path))
            return False

        chapter_format1_title = chapter_format1["title"]
        chapter_format2_title = book[book_format2 + "_chapters"][i]["title"]
        if chapter_format1_title != chapter_format2_title:
            debug("title different for chapter index: %d" % i)
            debug("format1=%s, title='%s'" % (book_format1, chapter_format1_title))
            debug("format2=%s, title='%s'" % (book_format2, chapter_format2_title))
            return False
    debug("chapter paths and titles the same.")
    return True


def _compare_manifest_entries(
    book: dict[str, Any],
    book_format1: str = "library",
    book_format2: str = "kobo",
):
    debug(
        "book_format1='%s', book_format2:'%s', count ToC entries: %d"
        % (book_format1, book_format2, len(book[book_format1 + "_manifest"]))
    )
    try:
        for i, manifest_item in enumerate(book[book_format1 + "_manifest"]):
            manifest_format1_path = manifest_item["path"]
            manifest_format2_path = book[book_format2 + "_manifest"][i]["path"]

            if manifest_format1_path != manifest_format2_path:
                debug("path different for manifest index: %d" % i)
                debug("format1=%s, path='%s'" % (book_format1, manifest_format1_path))
                debug("format2=%s, path='%s'" % (book_format2, manifest_format2_path))
                return False
        debug("manifest paths are same.")
        return True
    except Exception:
        return False


def update_device_toc_for_books(
    books: list[dict[str, Any]], device: KoboDevice, gui: ui.Main
):
    gui.status_bar.show_message(
        _("Updating ToC in device database for {0} books.").format(len(books)), 3000
    )
    debug("books=", books)
    progressbar = ProgressBar(
        parent=gui, window_title=_("Updating ToC in device database")
    )
    progressbar.set_label(_("Number of books to update: {0}").format(len(books)))
    progressbar.show_with_maximum(len(books))
    connection = utils.device_database_connection(device)
    for book in books:
        debug("book=", book)
        debug("ContentID=", book["ContentID"])
        progressbar.increment()

        if len(book["kobo_chapters"]) > 0:
            remove_all_toc_entries(connection, book["ContentID"])

            update_device_toc_for_book(
                connection,
                book,
                book["ContentID"],
                book["title"],
                book["kobo_format"],
            )

    progressbar.hide()


def update_device_toc_for_book(
    connection: DeviceDatabaseConnection,
    book: dict[str, Any],
    bookID: str,
    bookTitle: str,
    book_format: str = "EPUB",
):
    debug(
        "bookTitle=%s, len(book['library_chapters'])=%d"
        % (bookTitle, len(book["library_chapters"]))
    )
    num_chapters = len(book["kobo_chapters"])
    for i, chapter in enumerate(book["kobo_chapters"]):
        debug("chapter=", (chapter))
        if book_format == "KEPUB":
            chapterContentId = "{0}!{1}!{2}".format(
                book["ContentID"], book["kobo_opf_dir"], chapter["path"]
            )
        else:
            chapterContentId = book["ContentID"] + f"#({i})" + chapter["path"]
        debug("chapterContentId=", chapterContentId)
        databaseChapterId = getDatabaseChapterId(
            book["ContentID"], chapter["path"], connection
        )
        has_chapter = databaseChapterId is not None
        debug("has_chapter=", has_chapter)
        if (
            databaseChapterId is not None
            and chapter["path"].endswith("finish.xhtml")
            and chapterContentId != databaseChapterId
        ):
            debug("removing SOL finish chapter")
            removeChapterFromDatabase(databaseChapterId, bookID, connection)
            has_chapter = False
        if not has_chapter:
            addChapterToDatabase(
                chapterContentId,
                chapter,
                bookID,
                bookTitle,
                i,
                connection,
                book_format,
            )
            chapter["added"] = True

    if book_format == "KEPUB":
        num_chapters = len(book["kobo_manifest"])
        file_offset = 0
        total_file_size = sum(
            [manifest_entry["file_size"] for manifest_entry in book["kobo_manifest"]]
        )
        for i, manifest_entry in enumerate(book["kobo_manifest"]):
            file_size = manifest_entry["file_size"] * 100 / total_file_size
            manifest_entry_ContentId = "{0}!{1}!{2}".format(
                book["ContentID"][len("file://") :],
                book["kobo_opf_dir"],
                manifest_entry["path"],
            )
            addManifestEntryToDatabase(
                manifest_entry_ContentId,
                bookID,
                bookTitle,
                manifest_entry["path"],
                i,
                connection,
                file_size=int(file_size),
                file_offset=int(file_offset),
            )
            file_offset += file_size

    update_database_content_entry(connection, book["ContentID"], num_chapters)
    return 0


def getDatabaseChapterId(
    bookId: str, toc_file: str, connection: DeviceDatabaseConnection
) -> str | None:
    cursor = connection.cursor()
    t = (f"{bookId}%{toc_file}%",)
    cursor.execute("select ContentID from Content where ContentID like ?", t)
    try:
        result = next(cursor)
        chapterContentId = result[0]
    except StopIteration:
        chapterContentId = None

    debug("chapterContentId=%s" % chapterContentId)
    return chapterContentId


def removeChapterFromDatabase(
    chapterContentId: str, bookID: str, connection: DeviceDatabaseConnection
):
    cursor = connection.cursor()
    t = (chapterContentId,)
    cursor.execute("delete from Content where ContentID = ?", t)
    t = (
        bookID,
        chapterContentId,
    )
    cursor.execute(
        "delete from volume_shortcovers where volumeId = ? and shortcoverId = ?", t
    )

    return


def update_database_content_entry(
    connection: DeviceDatabaseConnection, contentId: str, num_chapters: int
):
    cursor = connection.cursor()
    t = (contentId, num_chapters)
    cursor.execute("UPDATE content SET NumShortcovers = ? where ContentID = ?", t)

    return


def remove_all_toc_entries(connection: DeviceDatabaseConnection, contentId: str):
    debug("contentId=", contentId)

    cursor = connection.cursor()
    t = (contentId,)

    cursor.execute("DELETE FROM Content WHERE BookID = ?", t)
    cursor.execute("DELETE FROM volume_shortcovers WHERE volumeId = ?", t)

    return


def addChapterToDatabase(
    chapterContentId: str,
    chapter: dict[str, Any],
    bookID: str,
    bookTitle: str,
    volumeIndex: int,
    connection: DeviceDatabaseConnection,
    book_format: str = "EPUB",
):
    cursorContent = connection.cursor()
    insertContentQuery = (
        "INSERT INTO content "
        "(ContentID, ContentType, MimeType, BookID, BookTitle, Title, Attribution, adobe_location"
        ", IsEncrypted, FirstTimeReading, ParagraphBookmarked, BookmarkWordOffset, VolumeIndex, ___NumPages"
        ", ReadStatus, ___UserID, ___FileOffset, ___FileSize, ___PercentRead"
        ", Depth, ChapterIDBookmarked"
        ") VALUES ("
        "?, ?, ?, ?, ?, ?, null, ?"
        ", 'false', 'true', 0, 0, ?, -1"
        ", 0, ?, 0, 0, 0"
        ", ?, ?"
        ")"
    )

    if book_format == "KEPUB":
        mime_type = "application/x-kobo-epub+zip"
        content_type = 899
        content_userid = ""
        adobe_location = None
        matches = re.match(r"(?:file://)?((.*?)(?:\#.*)?(?:-\d+))$", chapterContentId)
        assert matches is not None
        debug("regex matches=", matches.groups())
        chapterContentId = chapterContentId[len("file://") :]
        chapterContentId = matches.group(1)
        fragment_start = chapterContentId.rfind("#")
        chapter_id_bookmarked = (
            chapterContentId
            if fragment_start < 0
            else chapterContentId[:fragment_start]
        )
        chapter_id_bookmarked = matches.group(2)
    else:
        mime_type = "application/epub+zip"
        content_type = 9
        content_userid = "adobe_user"
        chapter_id_bookmarked = None
        if "chapter_location" in chapter:
            adobe_location = chapter["chapter_location"]
        else:
            adobe_location = chapter["path"]

    insertContentData = (
        chapterContentId,
        content_type,
        mime_type,
        bookID,
        bookTitle,
        chapter["title"],
        adobe_location,
        volumeIndex,
        content_userid,
        chapter["toc_depth"],
        chapter_id_bookmarked,
    )

    debug("insertContentData=", insertContentData)
    cursorContent.execute(insertContentQuery, insertContentData)
    cursorContent.close()

    if book_format == "EPUB":
        cursorShortCover = connection.cursor()
        insertShortCoverQuery = "INSERT INTO volume_shortcovers (volumeId, shortcoverId, VolumeIndex) VALUES (?,?,?)"
        insertShortCoverData = (
            bookID,
            chapterContentId,
            volumeIndex,
        )
        debug("insertShortCoverData=", insertShortCoverData)
        cursorShortCover.execute(insertShortCoverQuery, insertShortCoverData)

        cursorShortCover.close()


def addManifestEntryToDatabase(
    manifest_entry: str,
    bookID: str,
    bookTitle: str,
    title: str,
    volumeIndex: int,
    connection: DeviceDatabaseConnection,
    file_size: int,
    file_offset: int,
):
    cursorContent = connection.cursor()
    insertContentQuery = (
        "INSERT INTO content "
        "(ContentID, ContentType, MimeType, BookID, BookTitle, Title, Attribution, adobe_location"
        ", IsEncrypted, FirstTimeReading, ParagraphBookmarked, BookmarkWordOffset, VolumeIndex, ___NumPages"
        ", ReadStatus, ___UserID, ___FileOffset, ___FileSize, ___PercentRead"
        ", Depth, ChapterIDBookmarked"
        ") VALUES ("
        "?, ?, ?, ?, ?, ?, null, ?"
        ", 'false', 'true', 0, 0, ?, -1"
        ", 0, ?, ?, ?, 0"
        ", ?, ?"
        ")"
    )

    mime_type = "application/xhtml+xml"
    content_type = 9
    content_userid = ""
    adobe_location = None

    insertContentData = (
        manifest_entry,
        content_type,
        mime_type,
        bookID,
        bookTitle,
        title,
        adobe_location,
        volumeIndex,
        content_userid,
        file_offset,
        file_size,
        0,
        None,
    )
    debug("insertContentData=", insertContentData)
    cursorContent.execute(insertContentQuery, insertContentData)

    cursorShortCover = connection.cursor()
    insertShortCoverQuery = "INSERT INTO volume_shortcovers (volumeId, shortcoverId, VolumeIndex) VALUES (?,?,?)"
    insertShortCoverData = (
        bookID,
        manifest_entry,
        volumeIndex,
    )
    debug("insertShortCoverData=", insertShortCoverData)
    cursorShortCover.execute(insertShortCoverQuery, insertShortCoverData)

    cursorContent.close()
    cursorShortCover.close()


class UpdateBooksToCDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        books: list[dict[str, Any]],
    ):
        super().__init__(parent, "kobo utilities plugin:update book toc dialog")
        self.setWindowTitle(GUI_NAME)

        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self, "toc.png", _("Update ToCs in device database")
        )
        layout.addLayout(title_layout)

        self.books_table = ToCBookListTableWidget(self)
        layout.addWidget(self.books_table)

        options_layout = QHBoxLayout()

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.update_button_clicked)
        button_box.rejected.connect(self.reject)
        update_button = button_box.button(QDialogButtonBox.StandardButton.Ok)
        assert update_button is not None
        update_button.setText(_("Update ToC"))
        update_button.setToolTip(_("Update ToC in device database for selected books."))

        remove_button = button_box.addButton(
            _("Remove"), QDialogButtonBox.ButtonRole.ActionRole
        )
        assert remove_button is not None
        remove_button.setToolTip(_("Remove selected books from the list"))
        remove_button.setIcon(utils.get_icon("list_remove.png"))
        remove_button.clicked.connect(self.remove_from_list)

        send_books_button = button_box.addButton(
            _("Send books"), QDialogButtonBox.ButtonRole.ActionRole
        )
        assert send_books_button is not None
        send_books_button.setToolTip(
            _("Send books to device that have been updated in the library.")
        )
        send_books_button.clicked.connect(self.send_books_clicked)

        select_all_button = button_box.addButton(
            _("Select all"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert select_all_button is not None
        select_all_button.clicked.connect(self._select_all_clicked)
        select_all_button.setToolTip(_("Select all books in the list."))

        select_books_to_send_button = button_box.addButton(
            _("Select books to send"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert select_books_to_send_button is not None
        select_books_to_send_button.clicked.connect(self._select_books_to_send_clicked)
        select_books_to_send_button.setToolTip(
            _("Select all books that need to be sent to the device.")
        )

        select_books_to_update_button = button_box.addButton(
            _("Select books to update"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert select_books_to_update_button is not None
        select_books_to_update_button.clicked.connect(
            self._select_books_to_update_clicked
        )
        select_books_to_update_button.setToolTip(_("Select all books in the list."))

        clear_all_button = button_box.addButton(
            _("Clear all"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert clear_all_button is not None
        clear_all_button.clicked.connect(self._clear_all_clicked)
        clear_all_button.setToolTip(_("Unselect all books in the list."))

        options_layout.addWidget(button_box)

        layout.addLayout(options_layout)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()
        self.books_table.populate_table(books)

    def remove_from_list(self):
        self.books_table.remove_selected_rows()

    def send_books_clicked(self):
        books_to_send = self.books_table.books_to_send
        ids_to_sync = [book["calibre_id"] for book in books_to_send]
        debug("ids_to_sync=", ids_to_sync)
        parent = self.parent()
        assert isinstance(parent, ui.Main)
        if not question_dialog(
            self.parent(),
            _("Update books"),
            "<p>"
            + _(
                "There are {0} books that need to be updated on the device. "
                "After the book has been sent to the device, you can run the check and update the ToC."
                "<br/>"
                "Do you want to send the books to the device?"
            ).format(len(ids_to_sync)),
            show_copy_button=False,
        ):
            return
        parent.sync_to_device(
            on_card=None, delete_from_library=False, send_ids=ids_to_sync
        )
        self.reject()

    def update_button_clicked(self):
        books_to_send = self.books_to_update_toc
        ids_to_sync = [book["calibre_id"] for book in books_to_send]
        debug("ids_to_sync=", ids_to_sync)
        if not question_dialog(
            self.parent(),
            _("Update books"),
            "<p>"
            + _(
                "There are {0} books that need to have their ToC updated on the device. "
                "Any selected books that have not been imported into the database on the device are ignored."
                "<br/>"
                "Do you want to update the ToC in the database on the device?"
            ).format(len(ids_to_sync)),
            show_copy_button=False,
        ):
            return
        self.accept()

    def _select_books_to_send_clicked(self):
        self.books_table.select_checkmarks_send()

    def _select_books_to_update_clicked(self):
        self.books_table.select_checkmarks_update_toc()

    def _select_all_clicked(self):
        self.books_table.toggle_checkmarks(Qt.CheckState.Checked)

    def _clear_all_clicked(self):
        self.books_table.toggle_checkmarks(Qt.CheckState.Unchecked)

    @property
    def books_to_update_toc(self):
        return self.books_table.books_to_update_toc


class ToCBookListTableWidget(QTableWidget):
    STATUS_COLUMN_NO = 0
    TITLE_COLUMN_NO = 1
    AUTHOR_COLUMN_NO = 2
    LIBRARY_CHAPTERS_COUNT_COLUMN_NO = 3
    LIBRARY_FORMAT_COLUMN_NO = 4
    KOBO_DISC_CHAPTERS_COUNT_COLUMN_NO = 5
    KOBO_DISC_FORMAT_COLUMN_NO = 6
    KOBO_DISC_STATUS_COLUMN_NO = 7
    SEND_TO_DEVICE_COLUMN_NO = 8
    KOBO_DATABASE_CHAPTERS_COUNT_COLUMN_NO = 9
    KOBO_DATABASE_STATUS_COLUMN_NO = 10
    UPDATE_TOC_COLUMN_NO = 11
    READING_POSITION_COLUMN_NO = 12
    STATUS_COMMENT_COLUMN_NO = 13

    HEADER_LABELS_DICT = MappingProxyType(
        {
            STATUS_COLUMN_NO: "",
            TITLE_COLUMN_NO: _("Title"),
            AUTHOR_COLUMN_NO: _("Author"),
            LIBRARY_CHAPTERS_COUNT_COLUMN_NO: _("Library ToC"),
            LIBRARY_FORMAT_COLUMN_NO: _("Library format"),
            KOBO_DISC_CHAPTERS_COUNT_COLUMN_NO: _("Kobo ToC"),
            KOBO_DISC_FORMAT_COLUMN_NO: _("Kobo format"),
            KOBO_DISC_STATUS_COLUMN_NO: _("Status"),
            SEND_TO_DEVICE_COLUMN_NO: _("Send"),
            KOBO_DATABASE_CHAPTERS_COUNT_COLUMN_NO: _("Kobo database ToC"),
            KOBO_DATABASE_STATUS_COLUMN_NO: _("Status"),
            UPDATE_TOC_COLUMN_NO: _("ToC"),
            READING_POSITION_COLUMN_NO: _("Reading position"),
            STATUS_COMMENT_COLUMN_NO: _("Comment"),
        }
    )

    def __init__(self, parent: QWidget):
        QTableWidget.__init__(self, parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.books = {}

    def populate_table(self, books: list[dict[str, Any]]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(books))
        header_labels = [
            self.HEADER_LABELS_DICT[header_index]
            for header_index in sorted(self.HEADER_LABELS_DICT.keys())
        ]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(True)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.hide()

        self.books: dict[int, dict[str, Any]] = {}
        for row, book in enumerate(books):
            self.populate_table_row(row, book)
            self.books[row] = book

        # turning True breaks up/down.  Do we need either sorting or up/down?
        self.setSortingEnabled(True)
        self.resizeColumnsToContents()
        self.setMinimumColumnWidth(1, 100)
        self.setMinimumColumnWidth(2, 100)
        self.setMinimumColumnWidth(3, 100)
        self.setMinimumSize(300, 0)
        self.sortItems(1)
        self.sortItems(0)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(self, row: int, book: dict[str, Any]):
        book_status = 0
        if book["good"]:
            icon = utils.get_icon("ok.png")
            book_status = 0
        else:
            icon = utils.get_icon("minus.png")
            book_status = 1
        if "icon" in book:
            icon = utils.get_icon(book["icon"])

        status_cell = IconWidgetItem(None, icon, book_status)
        status_cell.setData(Qt.ItemDataRole.UserRole, book_status)
        self.setItem(row, 0, status_cell)

        title_cell = ReadOnlyTableWidgetItem(book["title"])
        title_cell.setData(Qt.ItemDataRole.UserRole, row)
        self.setItem(row, self.TITLE_COLUMN_NO, title_cell)

        self.setItem(
            row,
            self.AUTHOR_COLUMN_NO,
            AuthorTableWidgetItem(book["author"], book["author_sort"]),
        )

        if "library_chapters" in book and len(book["library_chapters"]) > 0:
            library_chapters_count = ReadOnlyTableWidgetItem(
                str(len(book["library_chapters"]))
            )
            library_chapters_count.setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            )
            self.setItem(
                row, self.LIBRARY_CHAPTERS_COUNT_COLUMN_NO, library_chapters_count
            )

        if "library_format" in book:
            library_format = ReadOnlyTableWidgetItem(str(book["library_format"]))
            library_format.setTextAlignment(
                Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            )
            self.setItem(row, self.LIBRARY_FORMAT_COLUMN_NO, library_format)

        if "kobo_chapters" in book and len(book["kobo_chapters"]) > 0:
            kobo_chapters_count = ReadOnlyTableWidgetItem(
                str(len(book["kobo_chapters"]))
            )
            kobo_chapters_count.setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            )
            self.setItem(
                row, self.KOBO_DISC_CHAPTERS_COUNT_COLUMN_NO, kobo_chapters_count
            )

        if "kobo_format" in book:
            kobo_format = ReadOnlyTableWidgetItem(str(book["kobo_format"]))
            kobo_format.setTextAlignment(
                Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            )
            self.setItem(row, self.KOBO_DISC_FORMAT_COLUMN_NO, kobo_format)

        kobo_format_status = 0
        if "kobo_format_status" in book:
            if book["kobo_format_status"]:
                icon = utils.get_icon("ok.png")
                kobo_format_status = 0
            else:
                icon = utils.get_icon("sync.png")
                kobo_format_status = 1
            kobo_format_status_cell = IconWidgetItem(None, icon, kobo_format_status)
            kobo_format_status_cell.setData(
                Qt.ItemDataRole.UserRole, kobo_format_status
            )
            self.setItem(row, self.KOBO_DISC_STATUS_COLUMN_NO, kobo_format_status_cell)

        kobo_disc_status = kobo_format_status == 1 and not book["good"]
        kobo_disc_status_cell = CheckableTableWidgetItem(checked=kobo_disc_status)
        kobo_disc_status_cell.setData(Qt.ItemDataRole.UserRole, kobo_disc_status)
        self.setItem(row, self.SEND_TO_DEVICE_COLUMN_NO, kobo_disc_status_cell)

        if "kobo_database_chapters" in book and len(book["kobo_database_chapters"]) > 0:
            kobo_database_chapters_count = ReadOnlyTableWidgetItem(
                str(len(book["kobo_database_chapters"]))
            )
            kobo_database_chapters_count.setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            )
            self.setItem(
                row,
                self.KOBO_DATABASE_CHAPTERS_COUNT_COLUMN_NO,
                kobo_database_chapters_count,
            )

        kobo_database_status = 0
        icon_name = "window-close.png"
        if "kobo_database_status" in book:
            if not book["can_update_toc"]:
                kobo_database_status = 0
                icon_name = "window-close.png"
            elif book["kobo_database_status"]:
                kobo_database_status = 0
                icon_name = "ok.png"
            else:
                kobo_database_status = 1
                icon_name = "toc.png"
        icon = utils.get_icon(icon_name)
        kobo_database_status_cell = IconWidgetItem(None, icon, kobo_database_status)
        kobo_database_status_cell.setData(
            Qt.ItemDataRole.UserRole, kobo_database_status
        )
        self.setItem(
            row, self.KOBO_DATABASE_STATUS_COLUMN_NO, kobo_database_status_cell
        )

        update_toc = kobo_database_status == 1 and book["can_update_toc"]
        update_toc_cell = CheckableTableWidgetItem(checked=update_toc)
        update_toc_cell.setData(Qt.ItemDataRole.UserRole, update_toc)
        self.setItem(row, self.UPDATE_TOC_COLUMN_NO, update_toc_cell)

        if (
            "koboDatabaseReadingLocation" in book
            and len(book["koboDatabaseReadingLocation"]) > 0
        ):
            koboDatabaseReadingLocation = ReadOnlyTableWidgetItem(
                book["koboDatabaseReadingLocation"]
            )
            self.setItem(
                row, self.READING_POSITION_COLUMN_NO, koboDatabaseReadingLocation
            )

        comment_cell = ReadOnlyTableWidgetItem(book["comment"])
        self.setItem(row, self.STATUS_COMMENT_COLUMN_NO, comment_cell)

    @property
    def books_to_update_toc(self) -> list[dict[str, Any]]:
        books = []
        for row in range(self.rowCount()):
            if cast(
                "CheckableTableWidgetItem", self.item(row, self.UPDATE_TOC_COLUMN_NO)
            ).get_boolean_value():
                item = self.item(row, self.TITLE_COLUMN_NO)
                assert item is not None
                rnum = item.data(Qt.ItemDataRole.UserRole)
                book = self.books[rnum]
                if book["can_update_toc"]:
                    books.append(book)
        return books

    @property
    def books_to_send(self) -> list[dict[str, Any]]:
        books = []
        for row in range(self.rowCount()):
            if cast(
                "CheckableTableWidgetItem",
                self.item(row, self.SEND_TO_DEVICE_COLUMN_NO),
            ).get_boolean_value():
                item = self.item(row, self.TITLE_COLUMN_NO)
                assert item is not None
                rnum = item.data(Qt.ItemDataRole.UserRole)
                book = self.books[rnum]
                books.append(book)
        return books

    def remove_selected_rows(self):
        self.setFocus()
        selection_model = self.selectionModel()
        assert selection_model is not None
        rows = selection_model.selectedRows()
        if len(rows) == 0:
            return
        message = "<p>Are you sure you want to remove this book from the list?"
        if len(rows) > 1:
            message = (
                "<p>Are you sure you want to remove the selected %d books from the list?"
                % len(rows)
            )
        if not confirm(message, "kobo_utilities_plugin_tocupdate_delete_item", self):
            return
        first_sel_row = self.currentRow()
        for selrow in reversed(rows):
            self.removeRow(selrow.row())
        if first_sel_row < self.rowCount():
            self.select_and_scroll_to_row(first_sel_row)
        elif self.rowCount() > 0:
            self.select_and_scroll_to_row(first_sel_row - 1)

    def select_and_scroll_to_row(self, row: int):
        self.selectRow(row)
        self.scrollToItem(self.currentItem())

    def toggle_checkmarks(self, select: Qt.CheckState):
        for i in range(self.rowCount()):
            item = self.item(i, self.UPDATE_TOC_COLUMN_NO)
            assert item is not None
            item.setCheckState(select)
        for i in range(self.rowCount()):
            item = self.item(i, self.SEND_TO_DEVICE_COLUMN_NO)
            assert item is not None
            item.setCheckState(select)

    def select_checkmarks_send(self):
        for i in range(self.rowCount()):
            item = self.item(i, 1)
            assert item is not None
            rnum = item.data(Qt.ItemDataRole.UserRole)
            debug("rnum=%s, book=%s" % (rnum, self.books[rnum]))
            item = self.item(i, self.SEND_TO_DEVICE_COLUMN_NO)
            assert item is not None
            item.setCheckState(
                Qt.CheckState.Unchecked
                if self.books[rnum]["kobo_format_status"]
                else Qt.CheckState.Checked
            )

    def select_checkmarks_update_toc(self):
        for i in range(self.rowCount()):
            item = self.item(i, 1)
            assert item is not None
            book_no = item.data(Qt.ItemDataRole.UserRole)
            debug("book_no=%s, book=%s" % (book_no, self.books[book_no]))
            check_for_toc = (
                not self.books[book_no]["kobo_database_status"]
                and self.books[book_no]["can_update_toc"]
            )
            item = self.item(i, self.UPDATE_TOC_COLUMN_NO)
            assert item is not None
            item.setCheckState(
                Qt.CheckState.Checked if check_for_toc else Qt.CheckState.Unchecked
            )


class IconWidgetItem(ReadOnlyTextIconWidgetItem):
    def __init__(self, text: str | None, icon: QIcon, sort_key: int):
        super().__init__(text, icon)
        self.sort_key = sort_key

    # Qt uses a simple < check for sorting items, override this to use the sortKey
    def __lt__(self, other: Any):
        if isinstance(other, IconWidgetItem):
            return self.sort_key < other.sort_key
        return NotImplemented


class AuthorTableWidgetItem(ReadOnlyTableWidgetItem):
    def __init__(self, text: str, sort_key: str):
        ReadOnlyTableWidgetItem.__init__(self, text)
        self.sort_key = sort_key

    # Qt uses a simple < check for sorting items, override this to use the sortKey
    def __lt__(self, other: Any):
        if isinstance(other, AuthorTableWidgetItem):
            return self.sort_key < other.sort_key
        return NotImplemented
