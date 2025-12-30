from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from calibre import strftime
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.library.delegates import DateDelegate
from qt.core import (
    QAbstractItemView,
    QCheckBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QRadioButton,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..dialogs import (
    DateTableWidgetItem,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    RatingTableWidgetItem,
)
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def fix_duplicate_shelves(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    shelves = _get_shelf_count(device)
    dlg = FixDuplicateShelvesDialog(gui, shelves)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        debug("dialog cancelled")
        return

    options = cfg.plugin_prefs.fixDuplicatesOptionsStore
    debug(f"about to fix shelves - options={options}")

    starting_shelves, shelves_removed, finished_shelves = _remove_duplicate_shelves(
        device, gui, shelves, options
    )
    result_message = (
        _("Update summary:")
        + "\n\t"
        + _(
            "Starting number of collections={0}\n\tCollections removed={1}\n\tTotal collections={2}"
        ).format(starting_shelves, shelves_removed, finished_shelves)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Duplicate collections fixed"),
        result_message,
        show=True,
    )


class FixDuplicateShelvesDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        shelves: list[list[Any]],
    ):
        super().__init__(
            parent,
            "kobo utilities plugin:duplicate shelves in device database dialog",
        )
        self.shelves = shelves
        self.blockSignals(True)

        self.initialize_controls()

        # Display the books in the table
        self.blockSignals(False)
        self.shelves_table.populate_table(self.shelves)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        options = cfg.plugin_prefs.fixDuplicatesOptionsStore
        self.setWindowTitle(_("Duplicate collections in device database"))
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/manage_series.png",
            _("Duplicate collections in device database"),
            "FixDuplicateCollections",
        )
        layout.addLayout(title_layout)

        # Main series table layout
        table_layout = QHBoxLayout()
        layout.addLayout(table_layout)

        self.shelves_table = DuplicateShelvesInDeviceDatabaseTableWidget(self)
        table_layout.addWidget(self.shelves_table)

        options_group = QGroupBox(_("Options"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        options_layout.addWidget(QLabel(_("Collection to keep")), 0, 0, 1, 1)
        self.keep_oldest_radiobutton = QRadioButton(_("Oldest"), self)
        options_layout.addWidget(self.keep_oldest_radiobutton, 0, 1, 1, 1)
        self.keep_oldest_radiobutton.setEnabled(True)

        self.keep_newest_radiobutton = QRadioButton(_("Newest"), self)
        options_layout.addWidget(self.keep_newest_radiobutton, 0, 2, 1, 1)
        self.keep_newest_radiobutton.setEnabled(True)

        if options.keepNewestShelf:
            self.keep_newest_radiobutton.click()
        else:
            self.keep_oldest_radiobutton.click()

        self.purge_checkbox = QCheckBox(_("Purge duplicate collections"), self)
        self.purge_checkbox.setToolTip(
            _(
                "When this option is selected, the duplicated rows are deleted from the database. "
                "If this is done, they might be restore during the next sync to the Kobo server."
            )
        )
        if options.purgeShelves:
            self.purge_checkbox.click()
        options_layout.addWidget(self.purge_checkbox, 0, 3, 1, 1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _ok_clicked(self) -> None:
        have_options = (
            self.keep_newest_radiobutton.isChecked()
            or self.keep_oldest_radiobutton.isChecked()
            or self.purge_checkbox.isChecked()
        )
        # Only if the user has checked at least one option will we continue
        if have_options:
            with cfg.plugin_prefs.fixDuplicatesOptionsStore as options:
                options.keepNewestShelf = self.keep_newest_radiobutton.isChecked()
                options.purgeShelves = self.purge_checkbox.isChecked()

            debug("options=%s" % options)
            self.accept()
            return
        error_dialog(
            self,
            _("No options selected"),
            _("You must select at least one option to continue."),
            show=True,
            show_copy_button=False,
        )


class DuplicateShelvesInDeviceDatabaseTableWidget(QTableWidget):
    def __init__(self, parent: QWidget):
        QTableWidget.__init__(self, parent)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)

    def populate_table(self, shelves: list[list[Any]]):
        self.clear()
        self.setAlternatingRowColors(True)
        self.setRowCount(len(shelves))
        header_labels = [
            _("Collection name"),
            _("Oldest"),
            _("Newest"),
            _("Number"),
        ]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.setDefaultSectionSize(24)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(True)

        for row, shelf in enumerate(shelves):
            self.populate_table_row(row, shelf)

        self.resizeColumnToContents(0)
        self.setMinimumColumnWidth(0, 150)
        self.setColumnWidth(1, 150)
        self.resizeColumnToContents(2)
        self.setMinimumColumnWidth(2, 150)
        self.setSortingEnabled(True)
        self.selectRow(0)
        delegate = DateDelegate(self)
        self.setItemDelegateForColumn(1, delegate)
        self.setItemDelegateForColumn(2, delegate)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(self, row: int, shelf: list[Any]):
        self.blockSignals(True)
        shelf_name = shelf[0] if shelf[0] else _("(Unnamed collection)")
        titleColumn = QTableWidgetItem(shelf_name)
        titleColumn.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
        self.setItem(row, 0, titleColumn)
        self.setItem(
            row,
            1,
            DateTableWidgetItem(shelf[1], is_read_only=True, default_to_today=False),
        )
        self.setItem(
            row,
            2,
            DateTableWidgetItem(shelf[2], is_read_only=True, default_to_today=False),
        )
        shelf_count = RatingTableWidgetItem(shelf[3], is_read_only=True)
        shelf_count.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.setItem(row, 3, shelf_count)
        self.blockSignals(False)


def _get_shelf_count(device: KoboDevice) -> list[list[Any]]:
    connection = utils.device_database_connection(device)
    shelves = []

    shelves_query = (
        "SELECT Name, MIN(CreationDate), MAX(CreationDate), COUNT(*), MAX(Id) "
        "FROM Shelf "
        "WHERE _IsDeleted = 'false' "
        "GROUP BY Name"
    )

    cursor = connection.cursor()
    cursor.execute(shelves_query)
    for i, row in enumerate(cursor):
        debug("row:", i, row[0], row[1], row[2], row[3], row[4])
        shelves.append(
            [
                row[0],
                utils.convert_kobo_date(row[1]),
                utils.convert_kobo_date(row[2]),
                int(row[3]),
                row[4],
            ]
        )

    return shelves


def _remove_duplicate_shelves(
    device: KoboDevice,
    gui: ui.Main,
    shelves: list[list[Any]],
    options: cfg.FixDuplicatesOptionsStoreConfig,
):
    debug("total shelves=%d: options=%s" % (len(shelves), options))
    starting_shelves = 0
    shelves_removed = 0
    finished_shelves = 0
    progressbar = ProgressBar(
        parent=gui, window_title=_("Duplicate collections in device database")
    )
    total_shelves = len(shelves)
    progressbar.show_with_maximum(total_shelves)
    progressbar.left_align_label()

    shelves_update_timestamp = (
        "UPDATE Shelf "
        "SET _IsDeleted = 'true', "
        "LastModified = ? "
        "WHERE _IsSynced = 'true' "
        "AND Name = ? "
        "AND CreationDate <> ?"
    )
    shelves_update_id = (
        "UPDATE Shelf "
        "SET _IsDeleted = 'true', "
        "LastModified = ? "
        "WHERE _IsSynced = 'true' "
        "AND Name = ? "
        "AND id <> ?"
    )

    shelves_delete_timestamp = (
        "DELETE FROM Shelf "
        "WHERE _IsSynced = 'false' "
        "AND Name = ? "
        "AND CreationDate <> ? "
        "AND _IsDeleted = 'true'"
    )
    shelves_delete_id = (
        "DELETE FROM Shelf "
        "WHERE _IsSynced = 'false' "
        "AND Name = ? "
        "AND id <> ?"
        "AND _IsDeleted = 'true'"
    )

    shelves_purge = "DELETE FROM Shelf WHERE _IsDeleted = 'true'"

    purge_shelves = options.purgeShelves
    keep_newest = options.keepNewestShelf

    with utils.device_database_connection(device) as connection:
        cursor = connection.cursor()
        for shelf in shelves:
            starting_shelves += shelf[3]
            finished_shelves += 1
            progressbar.set_label(
                _("Removing duplicates of collection {}").format(shelf[0])
            )
            progressbar.increment()

            if shelf[3] > 1:
                debug(
                    "shelf: %s, '%s', '%s', '%s', '%s'"
                    % (shelf[0], shelf[1], shelf[2], shelf[3], shelf[4])
                )
                timestamp = shelf[2] if keep_newest else shelf[1]
                shelf_id = shelf[4] if shelf[1] == shelf[2] else None
                shelves_values = (
                    shelf[0],
                    timestamp.strftime(device.timestamp_string),
                )

                if shelf_id:
                    shelves_update_query = shelves_update_id
                    shelves_delete_query = shelves_delete_id
                    shelves_update_values = (
                        strftime(device.timestamp_string, time.gmtime()),
                        shelf[0],
                        shelf_id,
                    )
                    shelves_delete_values = (shelf[0], shelf_id)
                else:
                    shelves_update_query = shelves_update_timestamp
                    shelves_delete_query = shelves_delete_timestamp
                    shelves_update_values = (
                        strftime(device.timestamp_string, time.gmtime()),
                        shelf[0],
                        timestamp.strftime(device.timestamp_string),
                    )
                    shelves_delete_values = shelves_values
                debug("marking as deleted:", shelves_update_values)
                debug("shelves_update_query:", shelves_update_query)
                debug("shelves_delete_query:", shelves_delete_query)
                debug("shelves_delete_values:", shelves_delete_values)
                cursor.execute(shelves_update_query, shelves_update_values)
                cursor.execute(shelves_delete_query, shelves_delete_values)
                shelves_removed += shelf[3] - 1

        if purge_shelves:
            debug("purging all shelves marked as deleted")
            cursor.execute(shelves_purge)

    progressbar.hide()
    return starting_shelves, shelves_removed, finished_shelves
