from __future__ import annotations

from typing import TYPE_CHECKING, cast

from calibre.gui2 import error_dialog, info_dialog
from qt.core import QDialogButtonBox, QVBoxLayout

from .. import config as cfg
from .. import utils
from ..constants import GUI_NAME
from ..dialogs import (
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    ReadingStatusGroupBox,
)
from ..features import metadata
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def change_reading_status(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    books = utils.get_books_for_selected(gui)

    if len(books) == 0:
        return
    for book in books:
        debug("book:", book)
        book.contentIDs = [book.contentID]
    debug("books:", books)

    dlg = ChangeReadingStatusOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return
    options = dlg.options
    options.usePlugboard = False
    options.titleSort = False
    options.authourSort = False
    options.subtitle = False
    debug("options:", options)

    progressbar = ProgressBar(
        parent=gui, window_title=_("Changing reading status on device")
    )
    progressbar.show()

    updated_books, unchanged_books, not_on_device_books, count_books = (
        metadata.do_update_metadata(books, device, gui, progressbar, options)
    )
    result_message = (
        _("Update summary:")
        + "\n\t"
        + _(
            "Books updated={0}\n\tUnchanged books={1}\n\tBooks not on device={2}\n\tTotal books={3}"
        ).format(updated_books, unchanged_books, not_on_device_books, count_books)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Device library updated"),
        result_message,
        show=True,
    )


class ChangeReadingStatusOptionsDialog(PluginDialog):
    def __init__(self, parent: ui.Main):
        super().__init__(
            parent,
            "kobo utilities plugin:change reading status settings dialog",
        )
        self.options = cfg.MetadataOptionsConfig()

        self.initialize_controls()

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Change reading status in device library"),
            "ChangeReadingStatus",
        )
        layout.addLayout(title_layout)

        self.readingStatusGroupBox = ReadingStatusGroupBox(
            cast("ui.Main", self.parent())
        )
        layout.addWidget(self.readingStatusGroupBox)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self) -> None:
        self.options.setRreadingStatus = (
            self.readingStatusGroupBox.readingStatusIsChecked()
        )
        if self.options.setRreadingStatus:
            self.options.readingStatus = self.readingStatusGroupBox.readingStatus()
            if self.options.readingStatus < 0:
                error_dialog(
                    self,
                    "No reading status option selected",
                    "If you are changing the reading status, you must select an option to continue",
                    show=True,
                    show_copy_button=False,
                )
                return
            self.options.resetPosition = (
                self.readingStatusGroupBox.reset_position_checkbox.isChecked()
            )

        # Only if the user has checked at least one option will we continue
        for _key, val in self.options:
            if val:
                self.accept()
                return
        error_dialog(
            self,
            _("No options selected"),
            _("You must select at least one option to continue."),
            show=True,
            show_copy_button=False,
        )
