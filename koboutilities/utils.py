# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2011, Grant Drake <grant.drake@gmail.com>, 2012-2022 updates by David Forrester <davidfor@internode.on.net>"
__docformat__ = "restructuredtext en"

import datetime as dt
import inspect
import os
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, cast

import apsw
from calibre.constants import DEBUG, iswindows
from calibre.devices.kobo.books import Book
from calibre.gui2 import error_dialog, info_dialog, open_url
from calibre.gui2.device import DeviceJob
from calibre.gui2.library.views import DeviceBooksView
from calibre.utils.config import config_dir
from qt.core import QIcon, QPixmap, QUrl

from .constants import GUI_NAME, WIKI_URL

try:
    # timed_print got added in Calibre 7.2.0
    from calibre.gui2 import timed_print
except ImportError:
    timed_print = print

if TYPE_CHECKING:
    from types import TracebackType

    from calibre.db.legacy import LibraryDatabase
    from calibre.gui2 import ui
    from calibre.gui2.dialogs.message_box import MessageBox
    from calibre.gui2.library.models import DeviceBooksModel
    from calibre.gui2.library.views import BooksView
    from qt.core import QDialogButtonBox, QModelIndex, QPushButton, QWidget

    from .config import KoboDevice

# Global definition of our plugin name. Used for common functions that require this.
plugin_name = None
# Global definition of our plugin resources. Used to share between the xxxAction and xxxBase
# classes if you need any zip images to be displayed on the configuration dialog.
plugin_icon_resources = {}

Dispatcher = Callable[[Callable[[DeviceJob], None]], None]
LoadResources = Callable[[Iterable[str]], Dict[str, bytes]]


def debug(*args: Any):
    if DEBUG:
        frame = inspect.currentframe()
        assert frame is not None
        frame = frame.f_back
        assert frame is not None
        code = frame.f_code
        filename = code.co_filename.replace("calibre_plugins.", "")
        # co_qualname was added in Python 3.11
        funcname = getattr(code, "co_qualname", code.co_name)
        timed_print(
            f"[DEBUG] [{filename}:{funcname}:{frame.f_lineno}]",
            *args,
        )


def set_plugin_icon_resources(name: str, resources: dict[str, bytes]):
    """
    Set our global store of plugin name and icon resources for sharing between
    the InterfaceAction class which reads them and the ConfigWidget
    if needed for use on the customization dialog for this plugin.
    """
    global plugin_icon_resources, plugin_name
    plugin_name = name
    plugin_icon_resources = resources


def get_icon(icon_name: str | None):
    """
    Retrieve a QIcon for the named image from the zip file if it exists,
    or if not then from Calibre's image cache.
    """
    if icon_name:
        pixmap = get_pixmap(icon_name)
        if pixmap is None:
            # Look in Calibre's cache for the icon
            return QIcon(I(icon_name))
        return QIcon(pixmap)
    return QIcon()


def get_pixmap(icon_name: str):
    """
    Retrieve a QPixmap for the named image
    Any icons belonging to the plugin must be prefixed with 'images/'
    """
    global plugin_icon_resources, plugin_name

    if not icon_name.startswith("images/"):
        # We know this is definitely not an icon belonging to this plugin
        pixmap = QPixmap()
        pixmap.load(I(icon_name))
        return pixmap

    # Check to see whether the icon exists as a Calibre resource
    # This will enable skinning if the user stores icons within a folder like:
    # ...\AppData\Roaming\calibre\resources\images\Plugin Name\
    if plugin_name:
        local_images_dir = get_local_images_dir(plugin_name)
        local_image_path = os.path.join(
            local_images_dir, icon_name.replace("images/", "")
        )
        if os.path.exists(local_image_path):
            pixmap = QPixmap()
            pixmap.load(local_image_path)
            return pixmap

    # As we did not find an icon elsewhere, look within our zip resources
    if icon_name in plugin_icon_resources:
        pixmap = QPixmap()
        pixmap.loadFromData(plugin_icon_resources[icon_name])
        return pixmap
    return None


def get_local_images_dir(subfolder: str | None = None):
    """
    Returns a path to the user's local resources/images folder
    If a subfolder name parameter is specified, appends this to the path
    """
    images_dir = os.path.join(config_dir, "resources/images")
    if subfolder:
        images_dir = os.path.join(images_dir, subfolder)
    if iswindows:
        images_dir = os.path.normpath(images_dir)
    return images_dir


def row_factory(cursor: apsw.Cursor, row: apsw.SQLiteValues):
    return {k[0]: row[i] for i, k in enumerate(cursor.getdescription())}


# This is necessary for Calibre 8 if the driver copies the database
# to a temporary location due to filesystem limitations.
# Without a lock the copying can lead to data loss.
# In addition, transactions are generally useful when changing the device DB.
class DeviceDatabaseConnection(apsw.Connection):
    def __init__(
        self,
        database_path: str,
        device_db_path: str,
        is_db_copied: bool,
        use_row_factory: bool = False,
    ) -> None:
        self.__lock = None
        self.__copy_db: Callable[[apsw.Connection, str], None] = lambda *_args: None
        try:
            from calibre.devices.kobo.db import copy_db, kobo_db_lock

            self.__lock = kobo_db_lock
            self.__copy_db = copy_db
        except ImportError:
            pass
        super().__init__(database_path)
        if use_row_factory:
            self.setrowtrace(row_factory)
        self.__device_db_path = device_db_path
        self.__is_db_copied = is_db_copied

    def __enter__(self) -> apsw.Connection:
        if self.__lock is not None:
            self.__lock.acquire()
        return super().__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        try:
            suppress_exception = super().__exit__(exc_type, exc_value, tb)
            if self.__is_db_copied and (
                suppress_exception
                or (exc_type is None and exc_value is None and tb is None)
            ):
                self.__copy_db(self, self.__device_db_path)
        finally:
            if self.__lock is not None:
                self.__lock.release()
        return suppress_exception


def device_database_connection(
    device: KoboDevice, use_row_factory: bool = False
) -> DeviceDatabaseConnection:
    return DeviceDatabaseConnection(
        device.db_path,
        device.device_db_path,
        device.is_db_copied,
        use_row_factory,
    )


def check_device_database(database_path: str):
    connection = DeviceDatabaseConnection(
        database_path, database_path, is_db_copied=False
    )
    check_query = "PRAGMA integrity_check"
    cursor = connection.cursor()

    check_result = ""
    cursor.execute(check_query)
    result = cursor.fetchall()
    if result:
        for line in result:
            debug("result line=", line)
            check_result += "\n" + str(line[0])
    else:
        check_result = _("Execution of '%s' failed") % check_query

    return check_result


def convert_kobo_date(kobo_date: str | None) -> dt.datetime | None:
    if kobo_date is None:
        return None

    from calibre.utils.date import local_tz, utc_tz

    try:
        converted_date = dt.datetime.strptime(
            kobo_date, "%Y-%m-%dT%H:%M:%S.%f"
        ).replace(tzinfo=utc_tz)
        converted_date = dt.datetime.strptime(
            kobo_date[0:19], "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=utc_tz)
    except ValueError:
        try:
            converted_date = dt.datetime.strptime(
                kobo_date, "%Y-%m-%dT%H:%M:%S%+00:00"
            ).replace(tzinfo=utc_tz)
        except ValueError:
            try:
                converted_date = dt.datetime.strptime(
                    kobo_date.split("+")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=utc_tz)
            except ValueError:
                try:
                    converted_date = dt.datetime.strptime(
                        kobo_date.split("+")[0], "%Y-%m-%d"
                    ).replace(tzinfo=utc_tz)
                except ValueError:
                    try:
                        from calibre.utils.date import parse_date

                        converted_date = parse_date(kobo_date, assume_utc=True)
                    except ValueError:
                        # The date is in some unknown format. Return now in the local timezone
                        converted_date = dt.datetime.now(tz=local_tz)
                        debug(f"datetime.now() - kobo_date={kobo_date}'")
    return converted_date


def is_device_view(gui: ui.Main) -> bool:
    return isinstance(gui.current_view(), DeviceBooksView)


def check_device_is_ready(
    device: KoboDevice | None, gui: ui.Main, function_message: str
):
    if gui.job_manager.has_device_jobs(queued_also=True):
        error_dialog(
            gui,
            GUI_NAME,
            function_message + "<br/>" + _("Device jobs are running or queued."),
            show=True,
            show_copy_button=False,
        )
        return False

    if device is None:
        error_dialog(
            gui,
            GUI_NAME,
            function_message + "<br/>" + _("No device connected."),
            show=True,
            show_copy_button=False,
        )
        return False

    return True


def get_contentIDs_from_id(book_id: int, gui: ui.Main) -> list[str | None]:
    debug("book_id=", book_id)
    paths = []
    for x in ("memory", "card_a"):
        x = getattr(gui, x + "_view").model()
        paths += x.paths_for_db_ids({book_id}, as_map=True)[book_id]
    debug("paths=", paths)
    return [r.contentID for r in paths]


def get_selected_ids(gui: ui.Main) -> list[int]:
    current_view = gui.current_view()
    if current_view is None:
        return []
    rows: list[QModelIndex] = current_view.selectionModel().selectedRows()
    if not rows or len(rows) == 0:
        return []
    debug("gui.current_view().model()", current_view.model())
    return list(map(current_view.model().id, rows))


def convert_calibre_ids_to_books(
    db: LibraryDatabase, ids: Iterable[int], get_cover: bool = False
) -> list[Book]:
    books = []
    for book_id in ids:
        book = convert_calibre_id_to_book(db, book_id, get_cover=get_cover)
        books.append(book)
    return books


def convert_calibre_id_to_book(
    db: LibraryDatabase, book_id: int, get_cover: bool = False
) -> Book:
    mi = db.get_metadata(book_id, index_is_id=True, get_cover=get_cover)
    book = Book("", "lpath", title=mi.title, other=mi)
    book.calibre_id = mi.id
    return book


def get_device_paths_from_id(book_id: int, gui: ui.Main) -> list[str]:
    books = get_books_from_ids({book_id}, gui)
    return [book.path for book in books[book_id]]


def get_device_path_from_id(book_id: int, gui: ui.Main) -> str | None:
    paths = get_device_paths_from_id(book_id, gui)
    return paths[0] if paths else None


def get_device_path_from_contentID(
    device: KoboDevice, contentID: str, mimetype: str
) -> str:
    card = "carda" if contentID.startswith("file:///mnt/sd/") else "main"
    return device.driver.path_from_contentid(contentID, "6", mimetype, card)


def get_books_from_ids(book_ids: Iterable[int], gui: ui.Main) -> dict[int, list[Book]]:
    books = defaultdict(list)
    for view in (gui.memory_view, gui.card_a_view):
        model: DeviceBooksModel = view.model()
        view_books = cast(
            "dict[int, list[Book]]", model.paths_for_db_ids(book_ids, as_map=True)
        )
        for book_id in view_books:
            books[book_id].extend(view_books[book_id])
    debug("books=", books)
    return books


def get_books_for_selected(gui: ui.Main) -> list[Book]:
    view: DeviceBooksView | BooksView | None = gui.current_view()  # pyright: ignore[reportGeneralTypeIssues]
    if view is None:
        return []
    if isinstance(view, DeviceBooksView):
        rows = view.selectionModel().selectedRows()
        books = []
        for r in rows:
            book = view.model().db[view.model().map[r.row()]]
            book.calibre_id = r.row()
            books.append(book)
    else:
        books = []

    return books


def contentid_from_path(device: KoboDevice, path: str, content_type: int):
    main_prefix = device.driver._main_prefix
    assert isinstance(main_prefix, str), f"_main_prefix is type {type(main_prefix)}"
    if content_type == 6:
        extension = os.path.splitext(path)[1]
        if extension == ".kobo":
            ContentID = os.path.splitext(path)[0]
            # Remove the prefix on the file.  it could be either
            ContentID = ContentID.replace(main_prefix, "")
        elif extension == "":
            ContentID = path
            kepub_path = device.driver.normalize_path(".kobo/kepub/")
            assert kepub_path is not None
            ContentID = ContentID.replace(main_prefix + kepub_path, "")
        else:
            ContentID = path
            ContentID = ContentID.replace(main_prefix, "file:///mnt/onboard/")

        if device.driver._card_a_prefix is not None:
            ContentID = ContentID.replace(
                device.driver._card_a_prefix, "file:///mnt/sd/"
            )
    else:  # ContentType = 16
        ContentID = path
        ContentID = ContentID.replace(main_prefix, "file:///mnt/onboard/")
        if device.driver._card_a_prefix is not None:
            ContentID = ContentID.replace(
                device.driver._card_a_prefix, "file:///mnt/sd/"
            )
    return ContentID.replace("\\", "/")


def remove_extra_files(
    extra_imageids_files: set[str],
    imageids_files: dict[str, str],
    delete_extra_covers: bool,
    image_path: str,
    images_tree: bool = False,
) -> list[str]:
    extra_image_files = []
    from glob import glob

    debug("images_tree=%s" % (images_tree))
    for imageId in extra_imageids_files:
        image_path = imageids_files[imageId]
        debug("image_path=%s" % (image_path))
        debug("imageId=%s" % (imageId))
        escaped_path = os.path.join(image_path, imageId + "*")
        escaped_path = re.sub(r"([\[\]])", r"[\1]", escaped_path)
        debug("escaped_path:", escaped_path)
        for filename in glob(escaped_path):
            debug("filename=%s" % (filename))
            extra_image_files.append(os.path.basename(filename))
            if delete_extra_covers:
                os.unlink(filename)
        if images_tree and delete_extra_covers:
            debug("about to remove directory: image_path=%s" % image_path)
            try:
                os.removedirs(image_path)
                debug("removed path=%s" % (image_path))
            except Exception as e:
                debug("removed path exception=", e)

    return extra_image_files


def value_changed(old_value: Any | None, new_value: Any | None) -> bool:
    return (
        (old_value is not None and new_value is None)
        or (old_value is None and new_value is not None)
        or old_value != new_value
    )


def show_help(anchor: str | None = None):
    debug("anchor=", anchor)

    from calibre.utils.localization import get_lang

    lang = get_lang()
    url = WIKI_URL
    if lang == "fr":
        url += "/French"
    url = QUrl(url)
    if anchor is not None and anchor != "":
        url.setFragment(anchor)
    open_url(url)


def prompt_for_restart(parent: QWidget, title: str, message: str):
    dialog_box = cast(
        "MessageBox", info_dialog(parent, title, message, show_copy_button=False)
    )
    bb = cast("QDialogButtonBox", dialog_box.bb)  # type: ignore[reportAttributeAccessIssue]
    button = cast(
        "QPushButton", bb.addButton(_("Restart calibre now"), bb.ButtonRole.AcceptRole)
    )
    button.setIcon(QIcon(I("lt.png")))

    class Restart:
        do_restart = False

    def rf():
        Restart.do_restart = True

    button.clicked.connect(rf)
    dialog_box.set_details("")
    dialog_box.exec()
    button.clicked.disconnect()
    return Restart.do_restart
