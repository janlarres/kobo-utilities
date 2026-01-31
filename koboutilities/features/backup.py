from __future__ import annotations

import datetime as dt
import glob
import os
import pickle
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from calibre.gui2 import FileDialog
from qt.core import QFileDialog

from .. import utils
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from calibre.gui2.device import DeviceJob

    from .. import config as cfg
    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


@dataclass
class DatabaseBackupJobOptions:
    backup_store_config: cfg.BackupOptionsStoreConfig
    device_name: str
    serial_number: str
    device_path: Path


# Backup file names will be KoboReader-devicename-serialnumber-timestamp.zip
BACKUP_FILE_TEMPLATE = "KoboReader-{0}-{1}-{2}.zip"
BACKUP_PATHS = [
    ".adobe-digital-editions",
    ".kobo/Kobo/Kobo eReader.conf",
    ".kobo/BookReader.sqlite",
    ".kobo/KoboReader.sqlite",
    ".kobo/affiliate.conf",
    ".kobo/version",
]


def backup_device_database(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    fd = FileDialog(
        parent=gui,
        name="Kobo Utilities plugin:choose backup destination",
        title=_("Choose backup destination"),
        filters=[(_("SQLite database"), ["sqlite"])],
        add_all_files_filter=False,
        mode=QFileDialog.FileMode.AnyFile,
    )
    if not fd.accepted:
        return
    backup_file = fd.get_files()[0]

    if not backup_file:
        return

    debug("backup file selected=", backup_file)
    source_file = device.db_path
    shutil.copyfile(source_file, backup_file)


def auto_backup_device_database(
    device: KoboDevice, gui: ui.Main, dispatcher: Dispatcher
):
    debug("start")
    if not device.backup_config:
        debug("no backup configuration")
        return
    backup_config = device.backup_config

    dest_dir = backup_config.backupDestDirectory
    debug("destination directory=", dest_dir)
    if not dest_dir or len(dest_dir) == 0:
        debug("destination directory not set, not doing backup")
        return

    debug("about to get version info from device...")
    version_info = device.version_info
    debug("version_info=", version_info)
    serial_number = device.version_info.serial_no
    device_name = "".join(device.driver.gui_name.split())
    debug("device_information=", device.driver.get_device_information())
    debug("device_name=", device_name)
    debug(
        "backup_file_template=",
        BACKUP_FILE_TEMPLATE.format(device_name, serial_number, ""),
    )

    job_options = DatabaseBackupJobOptions(
        backup_config,
        device_name,
        serial_number,
        Path(str(device.driver._main_prefix)),
    )
    debug("backup_options=", job_options)

    args = [pickle.dumps(job_options)]
    desc = _("Backing up Kobo device database")
    gui.device_manager.create_job(
        device_database_backup_job,
        dispatcher(lambda job: _device_database_backup_completed(job, gui)),
        description=desc,
        args=args,
    )
    gui.status_bar.show_message(_("Kobo Utilities") + " - " + desc, 3000)

    debug("end")


def _device_database_backup_completed(job: DeviceJob, gui: ui.Main):
    if job.failed:
        gui.job_exception(job, dialog_title=_("Failed to back up device database"))
        return


def device_database_backup_job(backup_options_raw: bytes):
    debug("start")
    backup_options: DatabaseBackupJobOptions = pickle.loads(backup_options_raw)  # noqa: S301

    debug("backup_options=", backup_options)
    device_name = backup_options.device_name
    serial_number = backup_options.serial_number
    dest_dir = backup_options.backup_store_config.backupDestDirectory
    copies_to_keep = backup_options.backup_store_config.backupCopiesToKeepSpin
    device_path = backup_options.device_path

    now = dt.datetime.now()  # noqa: DTZ005

    if not check_do_backup(backup_options, now):
        return

    backup_timestamp = now.strftime("%Y%m%d-%H%M%S")
    backup_file_name = BACKUP_FILE_TEMPLATE.format(
        device_name, serial_number, backup_timestamp
    )

    with tempfile.TemporaryDirectory(prefix="koboutilities-backup-") as tmpdir:
        tmpdir = Path(tmpdir)
        debug(f"tmpdir={tmpdir}")

        for file in BACKUP_PATHS:
            src_path = device_path / file
            debug(f"src_path={src_path}")
            dst_path = tmpdir / src_path.relative_to(device_path)
            debug(f"dst_path={dst_path}")
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            if src_path.is_dir():
                shutil.copytree(src_path, dst_path)
            else:
                shutil.copyfile(src_path, dst_path)

        # Check if the database is corrupted, since we don't want to back up
        # a corrupted database
        check_result = utils.check_device_database(
            str(tmpdir / ".kobo/KoboReader.sqlite")
        )
        if check_result.split()[0] != "ok":
            debug("database is corrupt!")
            raise Exception(check_result)

        # Create the zip file archive
        backup_file_path = Path(dest_dir, backup_file_name)
        debug("backup_file_path=%s" % backup_file_path)
        shutil.make_archive(
            str(backup_file_path.parent / backup_file_path.stem), "zip", tmpdir
        )

    if copies_to_keep > 0:
        remove_old_backups(backup_options)
    else:
        debug("Manually managing backups")


def remove_old_backups(options: DatabaseBackupJobOptions) -> None:
    copies_to_keep = options.backup_store_config.backupCopiesToKeepSpin
    dest_dir = options.backup_store_config.backupDestDirectory
    debug("copies to keep:%s" % copies_to_keep)

    timestamp_filter = "{0}-{1}".format("[0-9]" * 8, "[0-9]" * 6)
    backup_file_search = BACKUP_FILE_TEMPLATE.format(
        options.device_name, options.serial_number, timestamp_filter
    )
    debug("backup_file_search=", backup_file_search)

    backup_file_search_path = os.path.join(dest_dir, backup_file_search)
    debug("backup_file_search_path=", backup_file_search_path)
    backup_files = glob.glob(backup_file_search_path)
    debug("backup_files=", backup_files[: len(backup_files) - copies_to_keep])
    debug("len(backup_files) - copies_to_keep=", len(backup_files) - copies_to_keep)

    if len(backup_files) - copies_to_keep > 0:
        for filename in sorted(backup_files)[: len(backup_files) - copies_to_keep]:
            debug("removing backup file:", filename)
            os.unlink(filename)

    debug("Removing old backups - finished")


def check_do_backup(options: DatabaseBackupJobOptions, now: dt.datetime) -> bool:
    if not options.backup_store_config.doDailyBackp:
        return True

    backup_file_search = now.strftime(
        BACKUP_FILE_TEMPLATE.format(
            options.device_name, options.serial_number, "%Y%m%d-" + "[0-9]" * 6
        )
    )
    debug("backup_file_search=", backup_file_search)
    dest_dir = options.backup_store_config.backupDestDirectory
    backup_file_search = os.path.join(dest_dir, backup_file_search)
    debug("backup_file_search=", backup_file_search)
    backup_files = glob.glob(backup_file_search)
    debug("backup_files=", backup_files)

    if len(backup_files) > 0:
        debug("Backup already done today")
        return False

    return True
