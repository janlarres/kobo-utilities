from __future__ import annotations

import re
from collections import OrderedDict
from functools import partial
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote_plus

from calibre.devices.usbms.driver import USBMS
from calibre.ebooks.metadata import fmt_sidx
from calibre.ebooks.metadata.book.base import Metadata
from calibre.gui2 import info_dialog, open_url, question_dialog
from calibre.gui2.complete2 import EditWithComplete
from calibre.gui2.library.delegates import DateDelegate
from calibre.utils.config import tweaks
from calibre.utils.date import format_date, qt_to_dt
from calibre.utils.icu import sort_key
from qt.core import (
    QAbstractItemView,
    QAction,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialogButtonBox,
    QDoubleSpinBox,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QUrl,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..dialogs import (
    AuthorsTableWidgetItem,
    DateTableWidgetItem,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    ReadOnlyTableWidgetItem,
)
from ..features import metadata
from ..utils import debug

if TYPE_CHECKING:
    import datetime as dt

    from calibre.devices.kobo.books import Book
    from calibre.gui2 import ui
    from qt.core import QDropEvent, QMouseEvent, QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def manage_series_on_device(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    series_columns = get_series_columns(gui)

    books = utils.get_books_for_selected(gui)
    debug("books[0].__class__=", books[0].__class__)

    if len(books) == 0:
        return
    seriesBooks = [SeriesBook(book, series_columns) for book in books]
    seriesBooks = sorted(seriesBooks, key=lambda k: k.sort_key(sort_by_name=True))
    debug("seriesBooks[0]._mi.__class__=", seriesBooks[0]._mi.__class__)
    debug("seriesBooks[0]._mi.kobo_series=", seriesBooks[0]._mi.kobo_series)
    debug(
        "seriesBooks[0]._mi.kobo_series_number=",
        seriesBooks[0]._mi.kobo_series_number,
    )
    debug("books:", seriesBooks)

    library_db = gui.library_view.model().db
    all_series = library_db.all_series()
    all_series.sort(key=lambda x: sort_key(x[1]))

    d = ManageSeriesDeviceDialog(gui, seriesBooks, all_series, series_columns)
    d.exec()
    if d.result() != d.DialogCode.Accepted:
        return

    debug("done series management - books:", seriesBooks)

    options = cfg.MetadataOptionsConfig()
    books = []
    for seriesBook in seriesBooks:
        debug("seriesBook._mi.contentID=", seriesBook._mi.contentID)
        if (
            seriesBook.is_title_changed()
            or seriesBook.is_pubdate_changed()
            or seriesBook.is_series_changed()
        ):
            book = seriesBook._mi
            book.series_index_string = seriesBook.series_index_string()
            book.kobo_series_number = seriesBook.series_index_string()  # pyright: ignore[reportAttributeAccessIssue]
            book.kobo_series = seriesBook.series_name()  # pyright: ignore[reportAttributeAccessIssue]
            book.contentIDs = [book.contentID]
            books.append(book)
            options.title = options.title or seriesBook.is_title_changed()
            options.series = options.series or seriesBook.is_series_changed()
            options.published_date = (
                options.published_date or seriesBook.is_pubdate_changed()
            )
            debug("seriesBook._mi.__class__=", seriesBook._mi.__class__)
            debug(
                "seriesBook.is_pubdate_changed()=%s" % seriesBook.is_pubdate_changed()
            )
            debug("book.kobo_series=", book.kobo_series)
            debug("book.kobo_series_number=", book.kobo_series_number)
            debug("book.series=", book.series)
            debug("book.series_index=%s" % book.series_index)

    if options.title or options.series or options.published_date:
        progressbar = ProgressBar(
            parent=gui,
            window_title=_("Updating series information on device"),
            on_top=True,
        )
        progressbar.show()
        updated_books, unchanged_books, not_on_device_books, count_books = (
            metadata.do_update_metadata(books, device, gui, progressbar, options)
        )

        debug("about to call sync_booklists")
        USBMS.sync_booklists(device.driver, (current_view.model().db, None, None))
        result_message = (
            _("Update summary:")
            + "\n\t"
            + _(
                "Books updated={0}\n\tUnchanged books={1}\n\tBooks not on device={2}\n\tTotal books={3}"
            ).format(updated_books, unchanged_books, not_on_device_books, count_books)
        )
    else:
        result_message = _("No changes made to series information.")
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Manage series on device"),
        result_message,
        show=True,
    )


def get_series_columns(gui: ui.Main) -> dict[str, str]:
    custom_columns = cast(
        "dict[str, dict[str, Any]]", gui.library_view.model().custom_columns
    )
    series_columns = OrderedDict()
    for key, column in list(custom_columns.items()):
        typ = column["datatype"]
        if typ == "series":
            series_columns[key] = column["name"]
    return series_columns


class ManageSeriesDeviceDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        books: list[SeriesBook],
        all_series: list[tuple[int, str]],
        series_columns: dict[str, str],
    ):
        super().__init__(parent, "kobo utilities plugin:series dialog")
        self.db = parent.library_view.model().db
        self.books = books
        self.all_series = all_series
        self.series_columns = series_columns
        self.blockSignals(True)

        self.initialize_controls()

        # Books will have been sorted by the Calibre series column
        # Choose the appropriate series column to be editing
        initial_series_column = "Series"
        self.series_column_combo.select_text(initial_series_column)
        if len(series_columns) == 0:
            # Will not have fired the series_column_changed event
            self.series_column_changed()
        # Renumber the books using the assigned series name/index in combos/spinbox
        self.renumber_series(display_in_table=False)

        # Display the books in the table
        self.blockSignals(False)
        self.series_table.populate_table(books)
        if len(str(self.series_combo.text()).strip()) > 0:
            self.series_table.setFocus()
        else:
            self.series_combo.setFocus()
        self.update_series_headers(initial_series_column)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(_("Manage series"))
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/manage_series.png",
            _("Manage series on device"),
        )
        layout.addLayout(title_layout)

        # Series name and start index layout
        series_name_layout = QHBoxLayout()
        layout.addLayout(series_name_layout)

        series_column_label = QLabel(_("Series &Column:"), self)
        series_name_layout.addWidget(series_column_label)
        self.series_column_combo = SeriesColumnComboBox(self, self.series_columns)
        self.series_column_combo.currentIndexChanged[int].connect(
            self.series_column_changed
        )
        series_name_layout.addWidget(self.series_column_combo)
        series_column_label.setBuddy(self.series_column_combo)
        series_name_layout.addSpacing(20)

        series_label = QLabel(_("Series &Name:"), self)
        series_name_layout.addWidget(series_label)
        self.series_combo = EditWithComplete(self)
        self.series_combo.setEditable(True)
        self.series_combo.setInsertPolicy(QComboBox.InsertPolicy.InsertAlphabetically)
        self.series_combo.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.series_combo.setMinimumContentsLength(25)
        self.series_combo.currentIndexChanged[int].connect(self.series_changed)
        self.series_combo.editTextChanged.connect(self.series_changed)
        self.series_combo.set_separator(None)
        series_label.setBuddy(self.series_combo)
        series_name_layout.addWidget(self.series_combo)
        series_name_layout.addSpacing(20)
        series_start_label = QLabel(_("&Start at:"), self)
        series_name_layout.addWidget(series_start_label)
        self.series_start_number = QSpinBox(self)
        self.series_start_number.setRange(0, 99000000)
        self.series_start_number.valueChanged[int].connect(self.series_start_changed)
        series_name_layout.addWidget(self.series_start_number)
        series_start_label.setBuddy(self.series_start_number)
        series_name_layout.insertStretch(-1)

        # Series name and start index layout
        formatting_layout = QHBoxLayout()
        layout.addLayout(formatting_layout)

        self.clean_title_checkbox = QCheckBox(_("Clean titles of Kobo books"), self)
        formatting_layout.addWidget(self.clean_title_checkbox)
        self.clean_title_checkbox.setToolTip(
            _(
                "Removes series information from the titles. For Kobo books, this is '(Series Name - #1)'"
            )
        )
        self.clean_title_checkbox.clicked.connect(self.clean_title_checkbox_clicked)

        # Main series table layout
        table_layout = QHBoxLayout()
        layout.addLayout(table_layout)

        self.series_table = SeriesTableWidget(self)
        self.series_table.itemSelectionChanged.connect(self.item_selection_changed)
        self.series_table.cellChanged[int, int].connect(self.cell_changed)

        table_layout.addWidget(self.series_table)
        table_button_layout = QVBoxLayout()
        table_layout.addLayout(table_button_layout)
        move_up_button = QToolButton(self)
        move_up_button.setToolTip(_("Move book up in series (Alt+Up)"))
        move_up_button.setIcon(utils.get_icon("arrow-up.png"))
        move_up_button.setShortcut(_("Alt+Up"))
        move_up_button.clicked.connect(self.move_rows_up)
        table_button_layout.addWidget(move_up_button)
        move_down_button = QToolButton(self)
        move_down_button.setToolTip(_("Move book down in series (Alt+Down)"))
        move_down_button.setIcon(utils.get_icon("arrow-down.png"))
        move_down_button.setShortcut(_("Alt+Down"))
        move_down_button.clicked.connect(self.move_rows_down)
        table_button_layout.addWidget(move_down_button)
        spacerItem1 = QSpacerItem(
            20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding
        )
        table_button_layout.addItem(spacerItem1)
        assign_index_button = QToolButton(self)
        assign_index_button.setToolTip(_("Lock to index value..."))
        assign_index_button.setIcon(utils.get_icon("images/lock.png"))
        assign_index_button.clicked.connect(self.assign_index)
        table_button_layout.addWidget(assign_index_button)
        clear_index_button = QToolButton(self)
        clear_index_button.setToolTip(_("Unlock series index"))
        clear_index_button.setIcon(utils.get_icon("images/lock_delete.png"))
        clear_index_button.clicked.connect(self.clear_index)
        table_button_layout.addWidget(clear_index_button)
        spacerItem2 = QSpacerItem(
            20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding
        )
        table_button_layout.addItem(spacerItem2)
        delete_button = QToolButton(self)
        delete_button.setToolTip(_("Remove book from the series list"))
        delete_button.setIcon(utils.get_icon("trash.png"))
        delete_button.clicked.connect(self.remove_book)
        table_button_layout.addWidget(delete_button)
        spacerItem3 = QSpacerItem(
            20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding
        )
        table_button_layout.addItem(spacerItem3)
        move_left_button = QToolButton(self)
        move_left_button.setToolTip(
            _("Move series index to left of decimal point (Alt+Left)")
        )
        move_left_button.setIcon(utils.get_icon("back.png"))
        move_left_button.setShortcut(_("Alt+Left"))
        move_left_button.clicked.connect(partial(self.series_indent_change, -1))
        table_button_layout.addWidget(move_left_button)
        move_right_button = QToolButton(self)
        move_right_button.setToolTip(
            _("Move series index to right of decimal point (Alt+Right)")
        )
        move_right_button.setIcon(utils.get_icon("forward.png"))
        move_right_button.setShortcut(_("Alt+Right"))
        move_right_button.clicked.connect(partial(self.series_indent_change, 1))
        table_button_layout.addWidget(move_right_button)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        keep_button = button_box.addButton(
            _(" &Restore original series "), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert keep_button is not None
        keep_button.clicked.connect(self.restore_original_series)

    def reject(self):
        debug("start")
        for book in self.books:
            book.revert_changes()
        super().reject()

    def series_column_changed(self):
        debug("start")
        series_column = self.series_column_combo.selected_value()
        SeriesBook.series_column = series_column
        # Choose a series name and series index from the first book in the list
        initial_series_name = ""
        initial_series_index = 1
        if len(self.books) > 0:
            first_book = self.books[0]
            initial_series_name = first_book.series_name()
            debug("initial_series_name='%s'" % initial_series_name)
            if initial_series_name is not None:
                debug("first_book.series_index()='%s'" % first_book.series_index())
                try:
                    initial_series_index = int(first_book.series_index())
                except Exception:
                    initial_series_index = 1
        # Populate the series name combo as appropriate for that column
        self.initialize_series_name_combo(series_column, initial_series_name)
        # Populate the series index spinbox with the initial value
        self.series_start_number.setProperty("value", initial_series_index)
        self.update_series_headers(series_column)
        if self.signalsBlocked():
            return
        self.renumber_series()

    def update_series_headers(self, series_column: str) -> None:
        if series_column == "Series":
            self.series_table.set_series_column_headers(series_column)
        else:
            header_text = self.series_columns[series_column]
            self.series_table.set_series_column_headers(header_text)

    def initialize_series_name_combo(
        self, series_column: str, series_name: str | None
    ) -> None:
        self.series_combo.clear()
        if series_name is None:
            series_name = ""
        values = self.all_series
        if series_column == "Series":
            self.series_combo.update_items_cache([x[1] for x in values])
            for i in values:
                _id, name = i
                self.series_combo.addItem(name)
        else:
            label = self.db.field_metadata.key_to_label(series_column)
            values = list(self.db.all_custom(label=label))
            values.sort(key=sort_key)
            self.series_combo.update_items_cache(values)
            for name in values:
                self.series_combo.addItem(name)
        self.series_combo.setEditText(series_name)

    def series_changed(self):
        if self.signalsBlocked():
            return
        self.renumber_series()

    def series_start_changed(self):
        if self.signalsBlocked():
            return
        self.renumber_series()

    def restore_original_series(self):
        # Go through the books and overwrite the indexes with the originals, fixing in place
        for book in self.books:
            if book.orig_series_index():
                book.set_assigned_index(book.orig_series_index())
                book.set_series_name(book.orig_series_name())
                book.set_series_index(book.orig_series_index())
        # Now renumber the whole series so that anything in between gets changed
        self.renumber_series()

    def clean_title(self, remove_series: bool):
        # Go through the books and clean the Kobo series from the title
        for book in self.books:
            if remove_series:
                series_in_title = re.findall(r"\(.*\)", book._orig_title)
                if len(series_in_title) > 0:
                    book._mi.title = book._orig_title.replace(
                        series_in_title[len(series_in_title) - 1], ""
                    )
            else:
                book._mi.title = book._orig_title
        # Now renumber the whole series so that anything in between gets changed
        self.renumber_series()

    def clean_title_checkbox_clicked(self, checked: bool):
        self.clean_title(checked)

    def renumber_series(self, display_in_table: bool = True):
        if len(self.books) == 0:
            return
        series_name = str(self.series_combo.currentText()).strip()
        series_index = float(str(self.series_start_number.value()))
        last_series_indent = 0
        for row, book in enumerate(self.books):
            book.set_series_name(series_name)
            series_indent = book.series_indent()
            assigned_index = book.assigned_index()
            if assigned_index is not None:
                series_index = assigned_index
            else:
                if series_indent >= last_series_indent:
                    if series_indent == 0:
                        if row > 0:
                            series_index += 1.0
                    elif series_indent == 1:
                        series_index += 0.1
                    else:
                        series_index += 0.01
                else:
                    # When series indent decreases, need to round to next
                    if series_indent == 1:
                        series_index = round(series_index + 0.05, 1)
                    else:  # series_indent == 0:
                        series_index = round(series_index + 0.5, 0)
            book.set_series_index(series_index)
            last_series_indent = series_indent
        # Now determine whether books have a valid index or not
        self.books[0].set_is_valid(True)
        for row in range(len(self.books) - 1, 0, -1):
            book = self.books[row]
            previous_book = self.books[row - 1]
            if book.series_index() <= previous_book.series_index():
                book.set_is_valid(False)
            else:
                book.set_is_valid(True)
        if display_in_table:
            for row, book in enumerate(self.books):
                self.series_table.populate_table_row(row, book)

    def assign_original_index(self):
        if len(self.books) == 0:
            return
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        for row in selection_model.selectedRows():
            book = self.books[row.row()]
            book.set_assigned_index(book.orig_series_index())
        self.renumber_series()
        self.item_selection_changed()

    def assign_index(self):
        if len(self.books) == 0:
            return
        auto_assign_value = None
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        for row in selection_model.selectedRows():
            book = self.books[row.row()]
            if auto_assign_value is not None:
                book.set_assigned_index(auto_assign_value)
                continue

            d = LockSeriesDialog(self, book.title(), book.series_index())
            d.exec()
            if d.result() != d.DialogCode.Accepted:
                break
            if d.assign_same_value():
                auto_assign_value = d.get_value()
                book.set_assigned_index(auto_assign_value)
            else:
                book.set_assigned_index(d.get_value())

        self.renumber_series()
        self.item_selection_changed()

    def clear_index(self, all_rows: bool = False):
        if len(self.books) == 0:
            return
        if all_rows:
            for book in self.books:
                book.set_assigned_index(None)
        else:
            selection_model = self.series_table.selectionModel()
            assert selection_model is not None
            for row in selection_model.selectedRows():
                book = self.books[row.row()]
                book.set_assigned_index(None)
        self.renumber_series()

    def remove_book(self):
        if not question_dialog(
            self,
            _("Are you sure?"),
            "<p>" + _("Remove the selected book(s) from the series list?"),
            show_copy_button=False,
        ):
            return
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        rows = selection_model.selectedRows()
        if len(rows) == 0:
            return
        selrows = sorted(row.row() for row in rows)
        first_sel_row = self.series_table.currentRow()
        for row in reversed(selrows):
            self.books.pop(row)
            self.series_table.removeRow(row)
        if first_sel_row < self.series_table.rowCount():
            self.series_table.select_and_scroll_to_row(first_sel_row)
        elif self.series_table.rowCount() > 0:
            self.series_table.select_and_scroll_to_row(first_sel_row - 1)
        self.renumber_series()

    def move_rows_up(self):
        self.series_table.setFocus()
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        rows = selection_model.selectedRows()
        if len(rows) == 0:
            return
        first_sel_row = rows[0].row()
        if first_sel_row <= 0:
            return
        # Workaround for strange selection bug in Qt which "alters" the selection
        # in certain circumstances which meant move down only worked properly "once"
        selrows = sorted(row.row() for row in rows)
        for selrow in selrows:
            self.series_table.swap_row_widgets(selrow - 1, selrow + 1)
            self.books[selrow - 1], self.books[selrow] = (
                self.books[selrow],
                self.books[selrow - 1],
            )

        scroll_to_row = first_sel_row - 1
        if scroll_to_row > 0:
            scroll_to_row = scroll_to_row - 1
        self.series_table.scrollToItem(self.series_table.item(scroll_to_row, 0))
        self.renumber_series()

    def move_rows_down(self):
        self.series_table.setFocus()
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        rows = selection_model.selectedRows()
        if len(rows) == 0:
            return
        last_sel_row = rows[-1].row()
        if last_sel_row == self.series_table.rowCount() - 1:
            return
        # Workaround for strange selection bug in Qt which "alters" the selection
        # in certain circumstances which meant move down only worked properly "once"
        selrows = sorted(row.row() for row in rows)
        for selrow in reversed(selrows):
            self.series_table.swap_row_widgets(selrow + 2, selrow)
            self.books[selrow + 1], self.books[selrow] = (
                self.books[selrow],
                self.books[selrow + 1],
            )

        scroll_to_row = last_sel_row + 1
        if scroll_to_row < self.series_table.rowCount() - 1:
            scroll_to_row = scroll_to_row + 1
        self.series_table.scrollToItem(self.series_table.item(scroll_to_row, 0))
        self.renumber_series()

    def series_indent_change(self, delta: int):
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        for row in selection_model.selectedRows():
            book = self.books[row.row()]
            series_indent = book.series_indent()
            if delta > 0:
                if series_indent < 2:
                    book.set_series_indent(series_indent + 1)
            else:
                if series_indent > 0:
                    book.set_series_indent(series_indent - 1)
            book.set_assigned_index(None)
        self.renumber_series()

    def sort_by(self, name: str):
        if name == "PubDate":
            self.books = sorted(
                self.books, key=lambda k: k.sort_key(sort_by_pubdate=True)
            )
        elif name == "Original Series Name":
            self.books = sorted(self.books, key=lambda k: k.sort_key(sort_by_name=True))
        else:
            self.books = sorted(self.books, key=lambda k: k.sort_key())
        self.renumber_series()

    def search_web(self, name: str):
        URLS = {
            "FantasticFiction": "http://www.fantasticfiction.co.uk/search/?searchfor=author&keywords={author}",
            "Goodreads": "http://www.goodreads.com/search/search?q={author}&search_type=books",
            "Google": "http://www.google.com/#sclient=psy&q=%22{author}%22+%22{title}%22",
            "Wikipedia": "http://en.wikipedia.org/w/index.php?title=Special%3ASearch&search={author}",
        }
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        for row in selection_model.selectedRows():
            book = self.books[row.row()]
            safe_title = self.convert_to_search_text(book.title())
            safe_author = self.convert_author_to_search_text(book.authors()[0])
            url = (
                URLS[name]
                .replace("{title}", safe_title)
                .replace("{author}", safe_author)
            )
            open_url(QUrl(url))

    def convert_to_search_text(self, text: str, encoding: str = "utf-8"):
        # First we strip characters we will definitely not want to pass through.
        # Periods from author initials etc do not need to be supplied
        text = text.replace(".", "")
        # Now encode the text using Python function with chosen encoding
        text = quote_plus(text.encode(encoding, "ignore"))
        # If we ended up with double spaces as plus signs (++) replace them
        return text.replace("++", "+")

    def convert_author_to_search_text(self, author: str, encoding: str = "utf-8"):
        # We want to convert the author name to FN LN format if it is stored LN, FN
        # We do this because some websites (Kobo) have crappy search engines that
        # will not match Adams+Douglas but will match Douglas+Adams
        # Not really sure of the best way of determining if the user is using LN, FN
        # Approach will be to check the tweak and see if a comma is in the name

        # Comma separated author will be pipe delimited in Calibre database
        fn_ln_author = author
        if author.find(",") > -1:
            # This might be because of a FN LN,Jr - check the tweak
            sort_copy_method = tweaks["author_sort_copy_method"]
            if sort_copy_method == "invert":
                # Calibre default. Hence "probably" using FN LN format.
                fn_ln_author = author
            else:
                # We will assume that we need to switch the names from LN,FN to FN LN
                parts = author.split(",")
                surname = parts.pop(0)
                parts.append(surname)
                fn_ln_author = " ".join(parts).strip()
        return self.convert_to_search_text(fn_ln_author, encoding)

    def cell_changed(self, row: int, column: int):
        book = self.books[row]
        item = self.series_table.item(row, column)
        assert item is not None
        if column == 0:
            book.set_title(str(item.text()).strip())
        elif column == 2:
            qtdate = item.data(Qt.ItemDataRole.DisplayRole)
            book.set_pubdate(qt_to_dt(qtdate, as_utc=False))

    def item_selection_changed(self):
        row = self.series_table.currentRow()
        if row == -1:
            return
        has_assigned_index = False
        selection_model = self.series_table.selectionModel()
        assert selection_model is not None
        for row in selection_model.selectedRows():
            book = self.books[row.row()]
            if book.assigned_index():
                has_assigned_index = True
        self.series_table.clear_index_action.setEnabled(has_assigned_index)
        if not has_assigned_index:
            for book in self.books:
                if book.assigned_index():
                    has_assigned_index = True
        self.series_table.clear_all_index_action.setEnabled(has_assigned_index)


class SeriesTableWidgetItem(ReadOnlyTableWidgetItem):
    def __init__(
        self,
        series_name: str | None,
        series_index: str,
        is_original: bool = False,
        assigned_index: float | None = None,
    ):
        if series_name:
            text = "%s [%s]" % (series_name, series_index)
            text = "%s - %s" % (series_name, series_index)
        else:
            text = ""
        ReadOnlyTableWidgetItem.__init__(self, text)
        if assigned_index is not None:
            self.setIcon(utils.get_icon("images/lock.png"))
            self.setToolTip(_("Value assigned by user"))
        if is_original:
            self.setForeground(Qt.GlobalColor.darkGray)


class SeriesColumnComboBox(QComboBox):
    def __init__(self, parent: QWidget, series_columns: dict[str, Any]):
        QComboBox.__init__(self, parent)
        self.series_columns = series_columns
        for key, column in series_columns.items():
            self.addItem("%s (%s)" % (key, column))
        self.insertItem(0, "Series")

    def select_text(self, selected_key: str):
        if selected_key == "Series":
            self.setCurrentIndex(0)
        else:
            for idx, key in enumerate(self.seriesColumns.keys()):  # type: ignore[reportAttributeAccessIssue]
                if key == selected_key:
                    self.setCurrentIndex(idx)
                    return

    def selected_value(self) -> str:
        if self.currentIndex() == 0:
            return "Series"
        return list(self.series_columns.keys())[self.currentIndex() - 1]


class SeriesTableWidget(QTableWidget):
    def __init__(self, parent: QWidget):
        QTableWidget.__init__(self, parent)
        self.create_context_menu()
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.setDropIndicatorShown(True)
        self.fmt = tweaks["gui_pubdate_display_format"]
        if self.fmt is None:
            self.fmt = "MMM yyyy"

    def create_context_menu(self):
        parent = self.parent()
        assert isinstance(parent, ManageSeriesDeviceDialog)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        self.assign_original_index_action = QAction(
            _("Lock original series index"), self
        )
        self.assign_original_index_action.setIcon(utils.get_icon("images/lock.png"))
        self.assign_original_index_action.triggered.connect(
            parent.assign_original_index
        )
        self.addAction(self.assign_original_index_action)
        self.assign_index_action = QAction(_("Lock series index..."), self)
        self.assign_index_action.setIcon(utils.get_icon("images/lock.png"))
        self.assign_index_action.triggered.connect(parent.assign_index)
        self.addAction(self.assign_index_action)
        self.clear_index_action = QAction(_("Unlock series index"), self)
        self.clear_index_action.setIcon(utils.get_icon("images/lock_delete.png"))
        self.clear_index_action.triggered.connect(
            partial(parent.clear_index, all_rows=False)
        )
        self.addAction(self.clear_index_action)
        self.clear_all_index_action = QAction(_("Unlock all series index"), self)
        self.clear_all_index_action.setIcon(utils.get_icon("images/lock_open.png"))
        self.clear_all_index_action.triggered.connect(
            partial(parent.clear_index, all_rows=True)
        )
        self.addAction(self.clear_all_index_action)
        sep2 = QAction(self)
        sep2.setSeparator(True)
        self.addAction(sep2)
        for name in ["PubDate", "Original Series Index", "Original Series Name"]:
            sort_action = QAction("Sort by " + name, self)
            sort_action.setIcon(utils.get_icon("images/sort.png"))
            sort_action.triggered.connect(partial(parent.sort_by, name))
            self.addAction(sort_action)
        sep3 = QAction(self)
        sep3.setSeparator(True)
        self.addAction(sep3)
        for name, icon in [
            ("FantasticFiction", "images/ms_ff.png"),
            ("Goodreads", "images/ms_goodreads.png"),
            ("Google", "images/ms_google.png"),
            ("Wikipedia", "images/ms_wikipedia.png"),
        ]:
            menu_action = QAction("Search %s" % name, self)
            menu_action.setIcon(utils.get_icon(icon))
            menu_action.triggered.connect(partial(parent.search_web, name))
            self.addAction(menu_action)

    def populate_table(self, books: list[SeriesBook]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(books))
        header_labels = ["Title", "Author(s)", "PubDate", "Series", "New Series"]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        self.verticalHeader().setDefaultSectionSize(24)  # type: ignore[reportOptionalMemberAccess]
        self.horizontalHeader().setStretchLastSection(True)  # type: ignore[reportOptionalMemberAccess]

        for row, book in enumerate(books):
            self.populate_table_row(row, book)

        self.resizeColumnToContents(0)
        self.setMinimumColumnWidth(0, 150)
        self.setColumnWidth(1, 100)
        self.resizeColumnToContents(2)
        self.setMinimumColumnWidth(2, 60)
        self.resizeColumnToContents(3)
        self.setMinimumColumnWidth(3, 120)
        self.setSortingEnabled(False)
        self.setMinimumSize(550, 0)
        self.selectRow(0)
        delegate = DateDelegate(self, tweak_name="gui_pubdate_display_format")
        self.setItemDelegateForColumn(2, delegate)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(self, row: int, book: SeriesBook):
        self.blockSignals(True)
        self.setItem(row, 0, TitleWidgetItem(book))
        self.setItem(row, 1, AuthorsTableWidgetItem(book.authors()))
        self.setItem(
            row,
            2,
            DateTableWidgetItem(
                book.pubdate(), is_read_only=False, default_to_today=False, fmt=self.fmt
            ),
        )
        self.setItem(
            row,
            3,
            SeriesTableWidgetItem(
                book.orig_series_name(),
                book.orig_series_index_string(),
                is_original=True,
            ),
        )
        self.setItem(
            row,
            4,
            SeriesTableWidgetItem(
                book.series_name(),
                book.series_index_string(),
                assigned_index=book.assigned_index(),
            ),
        )
        self.blockSignals(False)

    def swap_row_widgets(self, src_row: int, dest_row: int):
        self.blockSignals(True)
        self.insertRow(dest_row)
        for col in range(self.columnCount()):
            self.setItem(dest_row, col, self.takeItem(src_row, col))
        self.removeRow(src_row)
        self.blockSignals(False)

    def select_and_scroll_to_row(self, row: int):
        self.selectRow(row)
        self.scrollToItem(self.currentItem())

    def event_has_mods(self, event: QMouseEvent | None = None):
        mods = (
            event.modifiers() if event is not None else QApplication.keyboardModifiers()
        )
        return (
            mods & Qt.KeyboardModifier.ControlModifier
            or mods & Qt.KeyboardModifier.ShiftModifier
        )

    def mousePressEvent(self, e: QMouseEvent | None):
        assert e is not None
        ep = e.pos()
        selection_model = self.selectionModel()
        assert selection_model is not None
        if (
            self.indexAt(ep) not in selection_model.selectedIndexes()
            and e.button() == Qt.MouseButton.LeftButton
            and not self.event_has_mods()
        ):
            self.setDragEnabled(False)
        else:
            self.setDragEnabled(True)
        return QTableWidget.mousePressEvent(self, e)

    def dropEvent(self, event: QDropEvent | None):
        assert event is not None
        selection_model = self.selectionModel()
        assert selection_model is not None
        rows = selection_model.selectedRows()
        selrows = sorted(row.row() for row in rows)
        drop_row = self.rowAt(event.pos().y())  # type: ignore[reportAttributeAccessIssue]
        if drop_row == -1:
            drop_row = self.rowCount() - 1
        rows_before_drop = [idx for idx in selrows if idx < drop_row]
        rows_after_drop = [idx for idx in selrows if idx >= drop_row]

        parent = self.parent()
        assert isinstance(parent, ManageSeriesDeviceDialog)
        dest_row = drop_row
        for selrow in rows_after_drop:
            dest_row += 1
            self.swap_row_widgets(selrow + 1, dest_row)
            book = parent.books.pop(selrow)
            parent.books.insert(dest_row, book)

        dest_row = drop_row + 1
        for selrow in reversed(rows_before_drop):
            self.swap_row_widgets(selrow, dest_row)
            book = parent.books.pop(selrow)
            parent.books.insert(dest_row - 1, book)
            dest_row = dest_row - 1

        event.setDropAction(Qt.DropAction.CopyAction)
        # Determine the new row selection
        self.selectRow(drop_row)
        parent.renumber_series()

    def set_series_column_headers(self, text: str):
        item = self.horizontalHeaderItem(3)
        if item is not None:
            item.setText("Original " + text)
        item = self.horizontalHeaderItem(4)
        if item is not None:
            item.setText("New " + text)


class LockSeriesDialog(PluginDialog):
    def __init__(
        self,
        parent: QWidget,
        title: str,
        initial_value: float,
    ):
        super().__init__(parent, "Manage Series plugin:lock series dialog")
        self.initialize_controls(title, initial_value)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self, title: str, initial_value: float):
        self.setWindowTitle(_("Lock series index"))
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self, "images/lock32.png", _("Lock series index")
        )
        layout.addLayout(title_layout)

        layout.addSpacing(10)
        self.title_label = QLabel("Series index for book: '%s'" % title, self)
        layout.addWidget(self.title_label)

        hlayout = QHBoxLayout()
        layout.addLayout(hlayout)

        self.value_spinbox = QDoubleSpinBox(self)
        self.value_spinbox.setRange(0, 99000000)
        self.value_spinbox.setDecimals(2)
        self.value_spinbox.setValue(initial_value)
        self.value_spinbox.selectAll()
        hlayout.addWidget(self.value_spinbox, 0)
        hlayout.addStretch(1)

        self.assign_same_checkbox = QCheckBox(
            _("&Assign this index value to all remaining books"), self
        )
        layout.addWidget(self.assign_same_checkbox)
        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_value(self):
        return float(str(self.value_spinbox.value()))

    def assign_same_value(self):
        return self.assign_same_checkbox.isChecked()


class TitleWidgetItem(QTableWidgetItem):
    def __init__(self, book: SeriesBook):
        super().__init__(book.title())
        self.title_sort = book.title()
        if not book.is_valid():
            self.setIcon(utils.get_icon("dialog_warning.png"))
            self.setToolTip(_("You have conflicting or out of sequence series indexes"))
        elif book.id() is None:
            self.setIcon(utils.get_icon("add_book.png"))
            self.setToolTip(_("Empty book added to series"))
        elif (
            book.is_title_changed()
            or book.is_pubdate_changed()
            or book.is_series_changed()
        ):
            self.setIcon(utils.get_icon("format-list-ordered.png"))
            self.setToolTip(_("The book data has been changed"))
        else:
            self.setIcon(utils.get_icon("ok.png"))
            self.setToolTip(_("The series data is unchanged"))

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, TitleWidgetItem):
            return self.title_sort < other.title_sort
        return NotImplemented


def get_indent_for_index(series_index: float | None) -> int:
    if not series_index:
        return 0
    return len(str(series_index).split(".")[1].rstrip("0"))


class SeriesBook:
    series_column = "Series"

    def __init__(self, mi: Book, series_columns: dict[str, str]):
        debug("mi.series_index=", mi.series_index)
        self._orig_mi = Metadata(_("Unknown"), other=mi)
        self._mi = mi
        self._orig_title = mi.title
        self._orig_pubdate = cast("dt.datetime", self._mi.pubdate)
        self._orig_series = cast("str | None", self._mi.kobo_series)
        self.get_series_index()
        self._series_columns = series_columns
        self._assigned_indexes: dict[str, float | None] = {"Series": None}
        self._series_indents = {
            "Series": get_indent_for_index(cast("float", mi.series_index))
        }
        self._is_valid_index = True
        self._orig_custom_series = {}

        for key in self._series_columns:
            self._orig_custom_series[key] = mi.get_user_metadata(key, True)
            self._series_indents[key] = get_indent_for_index(self.series_index())
            self._assigned_indexes[key] = None

    def get_series_index(self) -> None:
        self._orig_series_index_string = None
        self._series_index_format = None
        try:
            debug("self._mi.kobo_series_number=%s" % self._mi.kobo_series_number)
            self._orig_series_index = (
                float(self._mi.kobo_series_number)
                if self._mi.kobo_series_number is not None
                else None
            )
        except ValueError:
            debug(
                "non numeric series - self._mi.kobo_series_number=%s"
                % self._mi.kobo_series_number
            )
            assert self._mi.kobo_series_number is not None
            numbers = re.findall(r"\d*\.?\d+", self._mi.kobo_series_number)
            if len(numbers) > 0:
                self._orig_series_index = float(numbers[0])
                self._orig_series_index_string = self._mi.kobo_series_number
                self._series_index_format = self._mi.kobo_series_number.replace(
                    numbers[0], "%g", 1
                )
            debug("self._orig_series_index=", self._orig_series_index)

    def revert_changes(self):
        debug("start")
        self._mi.title = self._orig_title
        if hasattr(self._mi, "pubdate"):
            self._mi.pubdate = self._orig_pubdate
        self._mi.series = self._mi.kobo_series
        self._mi.series_index = self._orig_series_index  # pyright: ignore[reportAttributeAccessIssue]

        return

    def id(self) -> int | None:
        if hasattr(self._mi, "id"):
            return cast("int", self._mi.id)
        return None

    def authors(self) -> list[str]:
        return self._mi.authors

    def title(self) -> str:
        return self._mi.title

    def set_title(self, title: str):
        self._mi.title = title

    def is_title_changed(self) -> bool:
        return self._mi.title != self._orig_title

    def pubdate(self) -> dt.datetime | None:
        if hasattr(self._mi, "pubdate"):
            return cast("dt.datetime", self._mi.pubdate)
        return None

    def set_pubdate(self, pubdate: dt.datetime):
        self._mi.pubdate = pubdate

    def is_pubdate_changed(self) -> bool:
        if hasattr(self._mi, "pubdate") and hasattr(self._orig_mi, "pubdate"):
            return self._mi.pubdate != self._orig_pubdate
        return False

    def is_series_changed(self) -> bool:
        if self._mi.series != self._orig_series:
            return True
        return self._mi.series_index != self._orig_series_index

    def orig_series_name(self) -> str | None:
        return self._orig_series

    def orig_series_index(self):
        debug("self._orig_series_index=", self._orig_series_index)
        debug("self._orig_series_index.__class__=", self._orig_series_index.__class__)
        return self._orig_series_index

    def orig_series_index_string(self):
        if self._orig_series_index_string is not None:
            return self._orig_series_index_string

        return fmt_sidx(self._orig_series_index)

    def series_name(self) -> str | None:
        return cast("str | None", self._mi.series)

    def set_series_name(self, series_name: str | None) -> None:
        self._mi.series = series_name

    def series_index(self) -> float:
        return cast("float", self._mi.series_index)

    def series_index_string(self) -> str:
        if self._series_index_format is not None:
            return self._series_index_format % self._mi.series_index
        return fmt_sidx(self._mi.series_index)

    def set_series_index(self, series_index: float | None):
        self._mi.series_index = series_index  # pyright: ignore[reportAttributeAccessIssue]
        self.set_series_indent(get_indent_for_index(series_index))

    def series_indent(self) -> int:
        return self._series_indents[self.series_column]

    def set_series_indent(self, index: int):
        self._series_indents[self.series_column] = index

    def assigned_index(self):
        return self._assigned_indexes[self.series_column]

    def set_assigned_index(self, index: float | None) -> None:
        self._assigned_indexes[self.series_column] = index

    def is_valid(self) -> bool:
        return self._is_valid_index

    def set_is_valid(self, is_valid_index: bool) -> None:
        self._is_valid_index = is_valid_index

    def sort_key(
        self, sort_by_pubdate: bool = False, sort_by_name: bool = False
    ) -> str:
        if sort_by_pubdate:
            pub_date = self.pubdate()
            if pub_date is not None and pub_date.year > 101:
                return format_date(pub_date, "yyyyMMdd")
        else:
            series = self.orig_series_name()
            series_number = (
                self.orig_series_index() if self.orig_series_index() is not None else -1
            )
            debug("series_number=", series_number)
            debug("series_number.__class__=", series_number.__class__)
            if series:
                if sort_by_name:
                    return "%s%06.2f" % (series, series_number)
                return "%06.2f%s" % (series_number, series)
        return ""
