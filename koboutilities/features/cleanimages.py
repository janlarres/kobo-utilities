from __future__ import annotations

import os
import pickle
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable

from calibre.devices.kobo.driver import KOBOTOUCH
from calibre.gui2 import info_dialog
from qt.core import (
    QCheckBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QProgressDialog,
    QTimer,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog
from ..utils import DeviceDatabaseConnection, debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from calibre.gui2.device import DeviceJob
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


@dataclass
class CleanImagesDirJobOptions:
    main_image_path: str
    sd_image_path: str
    database_path: str
    device_database_path: str
    is_db_copied: bool
    delete_extra_covers: bool
    images_tree: bool


def clean_images_dir(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del load_resources
    debug("start")
    debug("device.path", device.path)

    dlg = CleanImagesDirOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    main_prefix = device.driver._main_prefix
    assert isinstance(main_prefix, str), f"_main_prefix is type {type(main_prefix)}"
    if (
        isinstance(device.driver, KOBOTOUCH)
        and device.driver.fwversion >= device.driver.min_fwversion_images_tree
    ):
        main_image_path = os.path.join(main_prefix, ".kobo-images")
        sd_image_path = (
            os.path.join(device.driver._card_a_prefix, "koboExtStorage/images-cache/")
            if device.driver._card_a_prefix
            else None
        )
        images_tree = True
    else:
        main_image_path = os.path.join(main_prefix, ".kobo/images")
        sd_image_path = (
            os.path.join(device.driver._card_a_prefix, "koboExtStorage/images")
            if device.driver._card_a_prefix
            else None
        )
        images_tree = False

    options = CleanImagesDirJobOptions(
        str(device.driver.normalize_path(main_image_path)),
        str(device.driver.normalize_path(sd_image_path)),
        device.db_path,
        device.device_db_path,
        device.is_db_copied,
        cfg.plugin_prefs.cleanImagesDir.delete_extra_covers,
        images_tree,
    )
    debug("options=", options)
    CleanImagesDirProgressDialog(gui, options, dispatcher)


def _clean_images_dir_job(
    gui: ui.Main, options: CleanImagesDirJobOptions, dispatcher: Dispatcher
):
    debug("Start")

    func = "arbitrary_n"
    cpus = gui.job_manager.server.pool_size
    args = [
        do_clean_images_dir.__module__,
        do_clean_images_dir.__name__,
        (pickle.dumps(options), cpus),
    ]
    desc = _("Cleaning images directory")
    gui.job_manager.run_job(
        dispatcher(partial(_clean_images_dir_completed, gui, options)),
        func,
        args=args,
        description=desc,
    )
    gui.status_bar.show_message(_("Cleaning images directory") + "...")


def _clean_images_dir_completed(
    gui: ui.Main, options: CleanImagesDirJobOptions, job: DeviceJob
) -> None:
    if job.failed:
        gui.job_exception(
            job, dialog_title=_("Failed to check cover directory on device")
        )
        return
    extra_image_files = job.result
    extra_covers_count = len(extra_image_files["main_memory"]) + len(
        extra_image_files["sd_card"]
    )
    gui.status_bar.show_message(_("Checking cover directory completed"), 3000)

    details = ""
    if extra_covers_count == 0:
        msg = _("No extra files found")
    else:
        msg = _(
            "Kobo Utilities found <b>{0} extra cover(s)</b> in the cover directory."
        ).format(extra_covers_count)
        if options.delete_extra_covers:
            msg += "\n" + _("All files have been deleted.")
        if len(extra_image_files["main_memory"]):
            details += (
                "\n" + _("Extra files found in main memory images directory:") + "\n"
            )
            for filename in extra_image_files["main_memory"]:
                details += "\t%s\n" % filename

        if len(extra_image_files["sd_card"]):
            details += "\n" + _("Extra files found in SD card images directory:") + "\n"
            for filename in extra_image_files["sd_card"]:
                details += "\t%s\n" % filename

    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Finished"),
        msg,
        show_copy_button=True,
        show=True,
        det_msg=details,
    )


def do_clean_images_dir(
    options_raw: bytes,
    cpus: int,
    notification: Callable[[float, str], Any] = lambda _x, y: y,
):
    del cpus
    options: CleanImagesDirJobOptions = pickle.loads(options_raw)  # noqa: S301
    main_image_path = options.main_image_path
    sd_image_path = options.sd_image_path
    database_path = options.database_path
    device_database_path = options.device_database_path
    is_db_copied = options.is_db_copied

    notification(1 / 7, "Getting ImageIDs from main images directory")
    debug(
        "Getting ImageIDs from main images directory - Path is: '%s'"
        % (main_image_path)
    )
    imageids_files_main = _get_file_imageIds(main_image_path)

    notification(2 / 7, "Getting ImageIDs from SD card images directory")
    debug("Getting ImageIDs from SD images directory - Path is: '%s'" % (sd_image_path))
    imageids_files_sd = _get_file_imageIds(sd_image_path)

    notification(3 / 7, "Getting ImageIDs from device database.")
    debug("Getting ImageIDs from device database.")
    imageids_db = _get_imageId_set(database_path, device_database_path, is_db_copied)

    notification(4 / 7, "Checking/removing images from main images directory")
    extra_imageids_files_main = set(imageids_files_main.keys()) - imageids_db
    debug(
        "Checking/removing images from main images directory - Number of extra images: %d"
        % (len(extra_imageids_files_main))
    )
    extra_image_files_main = utils.remove_extra_files(
        extra_imageids_files_main,
        imageids_files_main,
        options.delete_extra_covers,
        main_image_path,
        images_tree=options.images_tree,
    )

    notification(5 / 7, "Checking/removing images from SD card images directory")
    extra_imageids_files_sd = set(imageids_files_sd.keys()) - imageids_db
    debug(
        "Checking/removing images from SD card images directory - Number of extra images: %d"
        % (len(extra_imageids_files_sd))
    )
    extra_image_files_sd = utils.remove_extra_files(
        extra_imageids_files_sd,
        imageids_files_sd,
        options.delete_extra_covers,
        sd_image_path,
        images_tree=options.images_tree,
    )

    extra_image_files: dict[str, list[str]] = {}
    extra_image_files["main_memory"] = extra_image_files_main
    extra_image_files["sd_card"] = extra_image_files_sd

    notification(7 / 7, "Cleaning images directory - Done")

    return extra_image_files


def _get_file_imageIds(image_path: str | None) -> dict[str, str]:
    imageids_files = {}
    if image_path:
        for path, _dirs, files in os.walk(image_path):
            for filename in files:
                if filename.find(" - N3_") > 0:
                    imageid = filename.split(" - N3_")[0]
                    imageids_files[imageid] = path
                    continue
                if filename.find(" - AndroidBookLoadTablet_Aspect") > 0:
                    imageid = filename.split(" - AndroidBookLoadTablet_Aspect")[0]
                    imageids_files[imageid] = path
                    continue
                debug("path=%s" % (path))
                debug("check_covers: not 'N3' file - filename=%s" % (filename))

    return imageids_files


def _get_imageId_set(
    database_path: str, device_database_path: str, is_db_copied: bool
) -> set[str]:
    connection = DeviceDatabaseConnection(
        database_path, device_database_path, is_db_copied, use_row_factory=True
    )
    imageId_query = (
        "SELECT DISTINCT ImageId "
        "FROM content "
        "WHERE ContentType = 6 OR ContentType = 901"
    )
    cursor = connection.cursor()

    cursor.execute(imageId_query)
    return {row["ImageId"] for row in cursor}


class CleanImagesDirOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget):
        super().__init__(
            parent,
            "kobo utilities plugin:clean images dir settings dialog",
        )
        self.initialize_controls()

        self.delete_extra_covers_checkbox.setChecked(
            cfg.plugin_prefs.cleanImagesDir.delete_extra_covers
        )

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Clean images directory"),
            "CleanImagesDir",
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("Clean images"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)
        self.delete_extra_covers_checkbox = QCheckBox(
            _("Delete extra cover image files"), self
        )
        self.delete_extra_covers_checkbox.setToolTip(
            _(
                "Check this if you want to delete the extra cover image files from the images directory on the device."
            )
        )
        options_layout.addWidget(self.delete_extra_covers_checkbox, 0, 0, 1, 1)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self):
        cfg.plugin_prefs.cleanImagesDir.delete_extra_covers = (
            self.delete_extra_covers_checkbox.isChecked()
        )
        self.accept()


class CleanImagesDirProgressDialog(QProgressDialog):
    def __init__(
        self,
        gui: ui.Main | None,  # TODO Can this actually be None?
        options: CleanImagesDirJobOptions,
        dispatcher: Dispatcher,
    ):
        QProgressDialog.__init__(self, "", "", 0, 0, gui)
        debug("init")
        self.setMinimumWidth(500)
        self.options = options
        self.gui = gui
        self.dispatcher = dispatcher
        self.setWindowTitle(_("Creating queue for checking images directory"))
        QTimer.singleShot(0, self.do_clean_images_dir_queue)
        self.exec()

    def do_clean_images_dir_queue(self):
        debug("start")
        if self.gui is None:
            # There is a nasty QT bug with the timers/logic above which can
            # result in the do_queue method being called twice
            return
        self.hide()

        # Queue a job to process these ePub books
        _clean_images_dir_job(self.gui, self.options, self.dispatcher)
