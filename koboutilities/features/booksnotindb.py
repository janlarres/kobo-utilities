from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, cast

from calibre.gui2.library.delegates import DateDelegate
from calibre.utils.config import tweaks
from qt.core import (
    QAbstractItemView,
    QDialogButtonBox,
    QHBoxLayout,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from .. import utils
from ..constants import BOOK_CONTENTTYPE
from ..dialogs import (
    AuthorsTableWidgetItem,
    DateTableWidgetItem,
    ImageTitleLayout,
    PluginDialog,
    TitleWidgetItem,
)
from ..utils import debug

if TYPE_CHECKING:
    from calibre.devices.kobo.books import Book
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources

load_translations()


def show_books_not_in_database(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None:
        return

    books = utils.get_books_for_selected(gui)

    if len(books) == 0:
        books = current_view.model().db

    books_not_in_database = _check_book_in_database(device, books)

    dlg = ShowBooksNotInDeviceDatabaseDialog(gui, books_not_in_database)
    dlg.show()


def _check_book_in_database(device: KoboDevice, books: list[Book]) -> list[Book]:
    connection = utils.device_database_connection(device)
    not_on_device_books = []

    imageId_query = (
        "SELECT 1 "
        "FROM content "
        "WHERE BookID is NULL "
        "AND ContentId = ?"
    )  # fmt: skip
    cursor = connection.cursor()

    for book in books:
        if not book.contentID:
            book.contentID = utils.contentid_from_path(  # pyright: ignore[reportAttributeAccessIssue]
                device, book.path, BOOK_CONTENTTYPE
            )

        query_values = (book.contentID,)
        cursor.execute(imageId_query, query_values)
        try:
            next(cursor)
        except StopIteration:
            debug("no match for contentId='%s'" % (book.contentID,))
            not_on_device_books.append(book)

    return not_on_device_books


class BooksNotInDeviceDatabaseTableWidget(QTableWidget):
    def __init__(self, parent: QWidget):
        QTableWidget.__init__(self, parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.fmt = cast("str | None", tweaks["gui_pubdate_display_format"])
        if self.fmt is None:
            self.fmt = "MMM yyyy"

    def populate_table(self, books: list[Book]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(books))
        header_labels = [
            _("Title"),
            _("Author(s)"),
            _("File path"),
            _("PubDate"),
            _("File timestamp"),
        ]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.setDefaultSectionSize(24)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(True)

        for row, book in enumerate(books):
            self.populate_table_row(row, book)

        self.resizeColumnToContents(0)
        self.setMinimumColumnWidth(0, 150)
        self.setColumnWidth(1, 100)
        self.resizeColumnToContents(2)
        self.setMinimumColumnWidth(2, 200)
        self.setSortingEnabled(True)
        self.setMinimumSize(550, 0)
        self.selectRow(0)
        delegate = DateDelegate(self, tweak_name="gui_pubdate_display_format")
        self.setItemDelegateForColumn(3, delegate)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(self, row: int, book: Book):
        self.blockSignals(True)
        titleColumn = TitleWidgetItem(book)
        titleColumn.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, 0, titleColumn)
        authorColumn = AuthorsTableWidgetItem(book.authors, book.author_sort)
        self.setItem(row, 1, authorColumn)
        pathColumn = QTableWidgetItem(book.path)
        pathColumn.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, 2, pathColumn)
        self.setItem(
            row,
            3,
            DateTableWidgetItem(
                cast("dt.datetime", book.pubdate),
                is_read_only=True,
                default_to_today=False,
                fmt=self.fmt,
            ),
        )
        self.setItem(
            row,
            4,
            DateTableWidgetItem(
                dt.datetime(
                    book.datetime[0],
                    book.datetime[1],
                    book.datetime[2],
                    book.datetime[3],
                    book.datetime[4],
                    book.datetime[5],
                    book.datetime[6],
                    dt.timezone.utc,
                ),
                is_read_only=True,
                default_to_today=False,
            ),
        )
        self.blockSignals(False)


class ShowBooksNotInDeviceDatabaseDialog(PluginDialog):
    def __init__(self, parent: ui.Main, books: list[Book]):
        super().__init__(
            parent,
            "kobo utilities plugin:not in device database dialog",
        )
        self.db = parent.library_view.model().db
        self.books = books
        self.blockSignals(True)

        self.initialize_controls()

        # Display the books in the table
        self.blockSignals(False)
        self.books_table.populate_table(books)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(_("Books not in device database"))
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/manage_series.png",
            _("Books not in device database"),
        )
        layout.addLayout(title_layout)

        # Main series table layout
        table_layout = QHBoxLayout()
        layout.addLayout(table_layout)

        self.books_table = BooksNotInDeviceDatabaseTableWidget(self)
        table_layout.addWidget(self.books_table)

        # Dialog buttons
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)
