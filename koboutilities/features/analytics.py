from __future__ import annotations

from typing import TYPE_CHECKING

import apsw
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.dialogs.message_box import ViewLog
from qt.core import QDialogButtonBox, QGridLayout, QGroupBox, QRadioButton, QVBoxLayout

from .. import utils
from ..constants import GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def block_analytics(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    # Some background info:
    # https://www.mobileread.com/forums/showpost.php?p=3934039&postcount=44
    debug("start")
    dlg = BlockAnalyticsOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    block_analytics_result = _block_analytics(device, dlg.createAnalyticsEventsTrigger)
    if block_analytics_result:
        info_dialog(
            gui,
            _("Kobo Utilities") + " - " + _("Block analytics events"),
            block_analytics_result,
            show=True,
        )
    else:
        result_message = _("Failed to block analytics events.")
        d = ViewLog(
            _("Kobo Utilities") + " - " + _("Block analytics events"),
            result_message,
            parent=gui,
        )
        d.exec()


def _block_analytics(device: KoboDevice, create_trigger: bool):
    connection = utils.device_database_connection(device)
    block_result = "The trigger on the AnalyticsEvents table has been removed."

    cursor = connection.cursor()

    cursor.execute("DROP TRIGGER IF EXISTS BlockAnalyticsEvents")
    # Delete the Extended drvier version if it is there.
    cursor.execute("DROP TRIGGER IF EXISTS KTE_BlockAnalyticsEvents")

    if create_trigger:
        try:
            cursor.execute("DELETE FROM AnalyticsEvents")
            debug("creating trigger.")
            trigger_query = (
                "CREATE TRIGGER IF NOT EXISTS BlockAnalyticsEvents "
                "AFTER INSERT ON AnalyticsEvents "
                "BEGIN "
                "DELETE FROM AnalyticsEvents; "
                "END"
            )
            cursor.execute(trigger_query)
        except apsw.SQLError as e:
            debug("exception=", e)
            block_result = None
        else:
            block_result = "AnalyticsEvents have been blocked in the database."

    return block_result


class BlockAnalyticsOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget):
        super().__init__(
            parent,
            "kobo utilities plugin:block analytics settings dialog",
        )
        self.createAnalyticsEventsTrigger = True

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
            _("Block analytics"),
            "BlockAnalyticsEvents",
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("AnalyticsEvents database trigger"), self)
        options_group.setToolTip(
            _("When an entry is added to the AnalyticsEvents, it will be removed.")
        )
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        self.create_trigger_radiobutton = QRadioButton(
            _("Create or change trigger"), self
        )
        self.create_trigger_radiobutton.setToolTip(
            _("To create or change the trigger, select this option.")
        )
        options_layout.addWidget(self.create_trigger_radiobutton, 1, 0, 1, 1)

        self.delete_trigger_radiobutton = QRadioButton(_("Delete trigger"), self)
        self.delete_trigger_radiobutton.setToolTip(
            _(
                "This will remove the existing trigger and let the device work as Kobo intended it."
            )
        )
        options_layout.addWidget(self.delete_trigger_radiobutton, 1, 1, 1, 1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self) -> None:
        # Only if the user has checked at least one option will we continue
        if (
            self.create_trigger_radiobutton.isChecked()
            or self.delete_trigger_radiobutton.isChecked()
        ):
            self.createAnalyticsEventsTrigger = (
                self.create_trigger_radiobutton.isChecked()
            )
            self.accept()
            return
        error_dialog(
            self,
            _("No options selected"),
            _("You must select at least one option to continue."),
            show=True,
            show_copy_button=False,
        )
