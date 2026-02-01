from __future__ import annotations

import os
from typing import TYPE_CHECKING

from calibre.gui2 import info_dialog
from calibre.gui2.dialogs.message_box import ViewLog

from .. import utils
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources

load_translations()


def check_device_database(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    check_result = utils.check_device_database(device.db_path)

    check_result = (
        _(
            "Result of running 'PRAGMA integrity_check' on database on the Kobo device:\n\n"
        )
        + check_result
    )

    d = ViewLog("Kobo Utilities - Device Database Check", check_result, parent=gui)
    d.exec()


def vacuum_device_database(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    debug("start")

    uncompressed_db_size = os.path.getsize(device.db_path)

    connection = utils.device_database_connection(device)
    connection.execute("VACUUM")

    compressed_db_size = os.path.getsize(device.db_path)
    result_message = _(
        "The database on the device has been compressed.\n\tOriginal size = {0}MB\n\tCompressed size = {1}MB"
    ).format(
        "%.3f" % (uncompressed_db_size / 1024 / 1024),
        "%.3f" % (compressed_db_size / 1024 / 1024),
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Compress device database"),
        result_message,
        show=True,
    )
