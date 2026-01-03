from __future__ import annotations

from typing import TYPE_CHECKING, Any

from calibre.gui2 import info_dialog, question_dialog
from qt.core import (
    QAbstractItemView,
    QButtonGroup,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QPushButton,
    QRadioButton,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog, ProgressBar, RatingTableWidgetItem
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def set_related_books(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    debug("start")
    shelves = []
    dlg = SetRelatedBooksDialog(gui, device, shelves)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        debug("dialog cancelled")
        return
    options = cfg.plugin_prefs.setRelatedBooksOptionsStore
    debug("options=%s" % options)
    if dlg.deleteAllRelatedBooks:
        _delete_related_books(device, gui)
        result_message = _("Deleted all related books for sideloaded books.")
    else:
        related_types = dlg.get_related_types()
        debug("related_types=", related_types)

        categories_count, books_count = _set_related_books(
            device, gui, related_types, options
        )
        result_message = (
            _("Update summary:")
            + "\n\t"
            + _("Number of series or authors={0}\n\tNumber of books={1}").format(
                categories_count, books_count
            )
        )

    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Set related books"),
        result_message,
        show=True,
    )


def _get_related_books_count(
    device: KoboDevice, related_category: int
) -> list[dict[str, Any]]:
    debug("order_shelf_type:", related_category)
    connection = utils.device_database_connection(device)
    related_books = []

    series_query = (
        "SELECT Series, count(*) "
        "FROM content c "
        "WHERE c.ContentType = 6 "
        "AND c.ContentID LIKE 'file%' "
        "AND c.Series IS NOT NULL "
        "GROUP BY Series"
    )
    authors_query = (
        "SELECT Attribution, count(*) "
        "FROM content c "
        "WHERE c.ContentType = 6 "
        "AND c.ContentID LIKE 'file%' "
        "GROUP BY Attribution"
    )

    related_books_queries = [series_query, authors_query]
    related_books_query = related_books_queries[related_category]
    debug("related_books_query:", related_books_query)

    cursor = connection.cursor()
    cursor.execute(related_books_query)

    for i, row in enumerate(cursor):
        debug("row:", i, row[0], row[1])
        shelf = {}
        shelf["name"] = row[0]
        shelf["count"] = int(row[1])
        related_books.append(shelf)

    debug("related_books:", related_books)
    return related_books


def _set_related_books(
    device: KoboDevice,
    gui: ui.Main,
    related_books: list[dict[str, Any]],
    options: cfg.SetRelatedBooksOptionsStoreConfig,
):
    debug("related_books:", related_books, " options:", options)

    categories_count = 0
    books_count = 0

    progressbar = ProgressBar(parent=gui, window_title=_("Set related books"))
    total_related_books = len(related_books)
    progressbar.show_with_maximum(total_related_books)
    progressbar.left_align_label()

    series_query = (
        "SELECT c.ContentID, c.Title, c.Attribution, Series, SeriesNumber "
        "FROM content c "
        "WHERE c.ContentType = 6 "
        "AND Series = ? "
        "AND ContentID LIKE 'file%' "
    )
    author_query = (
        "SELECT c.ContentID, c.Title, c.Attribution, Series, SeriesNumber "
        "FROM content c "
        "WHERE c.ContentType = 6 "
        "AND Attribution = ? "
        "AND ContentID LIKE 'file%' "
    )
    if options.relatedBooksType == cfg.RelatedBooksType.Series:
        get_query = series_query
    else:
        get_query = author_query
    insert_query = "INSERT INTO volume_tabs VALUES ( ?, ? )"
    delete_query = "DELETE FROM volume_tabs WHERE tabId = ? "

    with utils.device_database_connection(device, use_row_factory=True) as connection:
        cursor = connection.cursor()
        for related_type in related_books:
            progressbar.set_label(
                _("Setting related books for {}").format(related_type["name"])
            )
            progressbar.increment()

            categories_count += 1
            debug(
                "related_type=%s, count=%d"
                % (related_type["name"], related_type["count"])
            )
            if related_type["count"] <= 1:
                continue
            related_type_data = (related_type["name"],)
            debug("related_type_data:", related_type_data)
            cursor.execute(get_query, related_type_data)
            related_type_contentIds = []
            for i, row in enumerate(cursor):
                debug(
                    "row:",
                    i,
                    row["ContentID"],
                    row["Title"],
                    row["Attribution"],
                    row["Series"],
                    row["SeriesNumber"],
                )
                related_type_contentIds.append(row["ContentID"])

            debug("related_type_contentIds:", related_type_contentIds)
            for tab_contentId in related_type_contentIds:
                cursor.execute(delete_query, (tab_contentId,))
                books_count += 1
                for volume_contentId in related_type_contentIds:
                    if tab_contentId != volume_contentId:
                        insert_data = (volume_contentId, tab_contentId)
                        debug("insert_data:", insert_data)
                        cursor.execute(insert_query, insert_data)

    progressbar.hide()
    debug("end")
    return categories_count, books_count


def _delete_related_books(device: KoboDevice, gui: ui.Main) -> None:
    progressbar = ProgressBar(parent=gui, window_title=_("Delete related books"))
    progressbar.show_with_maximum(100)
    progressbar.left_align_label()

    connection = utils.device_database_connection(device)
    delete_query = (
        "DELETE FROM volume_tabs  WHERE tabId LIKE 'file%' OR volumeId LIKE 'file%' "
    )

    cursor = connection.cursor()
    progressbar.set_label(_("Delete related books"))
    progressbar.increment()

    cursor.execute(delete_query)

    progressbar.hide()
    debug("end")


class SetRelatedBooksDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        device: KoboDevice,
        related_types: list[dict[str, Any]],
    ):
        super().__init__(
            parent,
            "kobo utilities plugin:set related books dialog",
        )
        self.device = device
        self.related_types = related_types
        self.blockSignals(True)
        self.dialog_title = _("Set related books")

        self.initialize_controls()

        self.related_category = (
            cfg.plugin_prefs.setRelatedBooksOptionsStore.relatedBooksType
        )
        self.deleteAllRelatedBooks = False
        button = self.related_categories_option_button_group.button(
            self.related_category
        )
        assert button is not None
        button.setChecked(True)

        # Display the books in the table
        self.blockSignals(False)
        self.related_types_table.populate_table(self.related_types)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/manage_series.png",
            self.dialog_title,
            "SetRelatedBooks",
        )
        layout.addLayout(title_layout)

        related_categories_option_group_box = QGroupBox(_("Related books type"), self)
        layout.addWidget(related_categories_option_group_box)

        related_categories_options = {
            cfg.RelatedBooksType.Series.value: (
                _("Series"),
                _("The related books will be all books in a series."),
                True,
            ),
            cfg.RelatedBooksType.Authors.value: (
                _("Authors"),
                _("The related books will be all books by the same author."),
                False,
            ),
        }

        related_categories_option_group_box_layout = QHBoxLayout()
        related_categories_option_group_box.setLayout(
            related_categories_option_group_box_layout
        )
        self.related_categories_option_button_group = QButtonGroup(self)
        self.related_categories_option_button_group.buttonClicked.connect(
            self._related_categories_option_radio_clicked
        )
        self.related_categories_buttons = {}
        for idx, clean_option in enumerate(related_categories_options):
            clean_options = related_categories_options[clean_option]
            rdo = QRadioButton(clean_options[0], self)
            rdo.setToolTip(clean_options[1])
            self.related_categories_option_button_group.addButton(rdo)
            self.related_categories_option_button_group.setId(rdo, clean_option)
            related_categories_option_group_box_layout.addWidget(rdo)
            self.related_categories_buttons[rdo] = idx

        self.fetch_button = QPushButton(_("Get list"), self)
        self.fetch_button.setToolTip(
            _("Get the list of categories to use for the related books")
        )
        self.fetch_button.clicked.connect(self.fetch_button_clicked)
        related_categories_option_group_box_layout.addWidget(self.fetch_button)

        # Main series table layout
        table_layout = QHBoxLayout()
        layout.addLayout(table_layout)

        self.related_types_table = OrderSeriesShelvesTableWidget(self)
        self.related_types_table.header_labels = [
            _("Series/author name"),
            _("Number of books"),
        ]
        table_layout.addWidget(self.related_types_table)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._ok_clicked)
        button_box.rejected.connect(self.reject)
        remove_selected_button = button_box.addButton(
            _("Remove"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert remove_selected_button is not None
        remove_selected_button.setToolTip(
            _(
                "Remove the selected category from the list. This will mean related books will not be changed for that category."
            )
        )
        remove_selected_button.clicked.connect(self._remove_selected_clicked)
        delete_related_button = button_box.addButton(
            _("Delete all"), QDialogButtonBox.ButtonRole.ActionRole
        )
        assert delete_related_button is not None
        delete_related_button.setToolTip(
            _("Delete all related books for sideloaded books.")
        )
        delete_related_button.clicked.connect(self._delete_related_clicked)
        layout.addWidget(button_box)

    def _ok_clicked(self):
        cfg.plugin_prefs.setRelatedBooksOptionsStore.relatedBooksType = (
            cfg.RelatedBooksType(self.related_category)
        )
        self.accept()
        return

    def _related_categories_option_radio_clicked(self, button: QRadioButton):
        self.related_category = self.related_categories_buttons[button]

    def fetch_button_clicked(self):
        self.related_types = _get_related_books_count(
            self.device, self.related_category
        )
        self.related_types_table.populate_table(self.related_types)
        return

    def _remove_selected_clicked(self):
        self.related_types_table.remove_selected_rows()

    def _delete_related_clicked(self):
        mb = question_dialog(
            self,
            self.dialog_title,
            _("Do you want to remove related books for all sideloaded books?"),
            show_copy_button=False,
        )
        if not mb:
            return

        self.deleteAllRelatedBooks = True
        self.accept()
        return

    def get_related_types(self):
        return self.related_types_table.get_shelves()


class OrderSeriesShelvesTableWidget(QTableWidget):
    def __init__(self, parent: QWidget):
        QTableWidget.__init__(self, parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.header_labels = [_("Collection/series name"), _("Books in collection")]
        self.shelves = {}

    def populate_table(self, shelves: list[dict[str, Any]]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(shelves))
        self.setColumnCount(len(self.header_labels))
        self.setHorizontalHeaderLabels(self.header_labels)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.setDefaultSectionSize(24)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(True)

        self.shelves = {}
        for row, shelf in enumerate(shelves):
            self.populate_table_row(row, shelf)
            self.shelves[row] = shelf

        self.resizeColumnToContents(0)
        self.setMinimumColumnWidth(0, 150)
        self.setColumnWidth(1, 150)
        self.setSortingEnabled(True)
        self.selectRow(0)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(self, row: int, shelf: dict[str, Any]):
        self.blockSignals(True)
        nameColumn = QTableWidgetItem(shelf["name"])
        nameColumn.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
        nameColumn.setData(Qt.ItemDataRole.UserRole, row)
        self.setItem(row, 0, nameColumn)
        shelf_count = RatingTableWidgetItem(shelf["count"], is_read_only=True)
        shelf_count.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.setItem(row, 1, shelf_count)
        self.blockSignals(False)

    def get_shelves(self) -> list[dict[str, Any]]:
        shelves = []
        for row in range(self.rowCount()):
            rnum = self.item(row, 0).data(Qt.ItemDataRole.UserRole)  # type: ignore[reportOptionalMemberAccess]
            shelf = self.shelves[rnum]
            shelves.append(shelf)
        return shelves

    def remove_selected_rows(self):
        self.setFocus()
        rows = self.selectionModel().selectedRows()  # type: ignore[reportOptionalMemberAccess]
        if len(rows) == 0:
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
