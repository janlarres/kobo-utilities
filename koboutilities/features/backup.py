from __future__ import annotations

import datetime as dt
import glob
import os
import pickle
import re
import shutil
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, NewType

import apsw
from calibre.gui2 import FileDialog, error_dialog, info_dialog, question_dialog
from qt.core import QFileDialog

from .. import utils
from ..constants import CORRUPT_MSG
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
    # We need the DB path separately from the device path since it may have been
    # copied somewhere else due to filesystem limitations
    db_path: str
    device_path: Path


@dataclass
class BackupDeviceInfo:
    device_name: str
    serial_number: str
    fw_version: str


BackupInfo = NewType("BackupInfo", BackupDeviceInfo)
DeviceInfo = NewType("DeviceInfo", BackupDeviceInfo)


# Backup file names will be KoboReader-devicename-serialnumber-timestamp.zip
BACKUP_FILE_TEMPLATE = "KoboReader-{0}-{1}-{2}.zip"
BACKUP_PATHS = [
    ".adobe-digital-editions",
    ".kobo/Kobo/Kobo eReader.conf",
    ".kobo/BookReader.sqlite",
    ".kobo/affiliate.conf",
    ".kobo/version",
]

load_translations()


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
        device.db_path,
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
        if isinstance(job.exception, apsw.CorruptError):
            job.description += "<p>" + CORRUPT_MSG
        gui.job_exception(job, dialog_title=_("Failed to back up device database"))


def device_database_backup_job(backup_options_raw: bytes):
    debug("start")
    backup_options: DatabaseBackupJobOptions = pickle.loads(backup_options_raw)  # noqa: S301

    debug("backup_options=", backup_options)
    device_name = backup_options.device_name
    serial_number = backup_options.serial_number
    dest_dir = Path(backup_options.backup_store_config.backupDestDirectory)
    copies_to_keep = backup_options.backup_store_config.backupCopiesToKeepSpin
    db_path = backup_options.db_path
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

        db_tmp_path = str(tmpdir / ".kobo/KoboReader.sqlite")
        (tmpdir / ".kobo").mkdir(parents=True, exist_ok=True)

        # closing() is necessary here to close the connection at the end of this block
        # so that Windows doesn't complain when deleting the temporary directory
        # at the end of the outer block. Normally it would be closed when the
        # connection goes out of scope at the end of the function.
        # It should also be safer to close the connection before archiving the DB
        # so that all journal files are deleted.
        with apsw.Connection(db_path) as src, closing(
            apsw.Connection(db_tmp_path)
        ) as dest, dest.backup("main", src, "main") as backup:
            while not backup.done:
                backup.step()

        for file in BACKUP_PATHS:
            src_path = device_path / file
            debug(f"src_path={src_path}")
            dst_path = tmpdir / src_path.relative_to(device_path)
            debug(f"dst_path={dst_path}")
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            if not src_path.exists():
                debug(f"File {src_path} does not exist; not backing up")
            elif src_path.is_dir():
                shutil.copytree(src_path, dst_path)
            else:
                shutil.copyfile(src_path, dst_path)

        # Check if the database is corrupt since we don't want to back up
        # a corrupt database
        check_result = utils.check_device_database(db_tmp_path)
        if check_result.split()[0] != "ok":
            debug("database is corrupt!")
            raise apsw.CorruptError(check_result)

        # Create the zip file archive
        backup_file_path = dest_dir / backup_file_name
        debug("backup_file_path=%s" % backup_file_path)
        dest_dir.mkdir(parents=True, exist_ok=True)
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


def restore_backup(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources

    backup_dir = device.backup_config.backupDestDirectory
    fd = FileDialog(
        parent=gui,
        name="Kobo Utilities plugin:choose backup to restore",
        title=_("Choose backup to restore"),
        filters=[(_("Kobo backups"), ["zip"])],
        add_all_files_filter=False,
        default_dir=backup_dir,
        mode=QFileDialog.FileMode.ExistingFile,
    )
    if not fd.accepted:
        return
    backup_path = fd.get_files()[0]
    if not backup_path:
        return
    backup_path = Path(backup_path)
    debug(f"backup_file={backup_path}")

    match = re.match(
        BACKUP_FILE_TEMPLATE.format(
            r"(?P<device_name>[^-]+)",
            r"(?P<serial_number>[^-]+)",
            r"(?P<timestamp>\d{8}-\d{6})",
        ),
        backup_path.name,
    )
    if match is None:
        debug(f"Failed to parse filename {backup_path.name}")
        invalid_backup_dialog(backup_path.name, gui)
        return
    bk_device_name = match.group("device_name")
    bk_timestamp = dt.datetime.strptime(match.group("timestamp"), "%Y%m%d-%H%M%S")  # noqa: DTZ007

    with tempfile.TemporaryDirectory(prefix="koboutilities-restore-") as tmpdir:
        tmpdir = Path(tmpdir)
        debug(f"tmpdir={tmpdir}")
        do_restore(backup_path, tmpdir, device, bk_device_name, bk_timestamp, gui)


def do_restore(
    backup_path: Path,
    tmpdir: Path,
    device: KoboDevice,
    bk_device_name: str,
    bk_timestamp: dt.datetime,
    gui: ui.Main,
) -> None:
    shutil.unpack_archive(backup_path, tmpdir)

    if (
        not (tmpdir / ".kobo/KoboReader.sqlite").exists()
        or not (tmpdir / ".kobo/version").exists()
    ):
        debug("Failed to find critical files in backup")
        invalid_backup_dialog(backup_path.name, gui)
        return

    restore_msg = _(
        "Restoring this backup from {date} will overwrite the database"
        " and some configuration files on the device with the files from the backup."
        " Are you sure you want to proceed?"
    ).format(date=bk_timestamp.strftime("%c"))

    backup_info, device_info = get_backup_info(tmpdir, bk_device_name, device)
    debug(f"backup_info={backup_info}")
    debug(f"device_info={device_info}")

    if backup_info.device_name != device_info.device_name:
        debug("Incompatible backup")
        error_dialog(
            gui,
            _("Incompatible backup"),
            _(
                "This backup was created from a different device type. Restoring backups to a different device type is not supported."
            )
            + format_info_table(backup_info, device_info),
            show=True,
        )
        return
    if backup_info.serial_number != device_info.serial_number:
        restore_msg = (
            _(
                "This backup was created from a different device."
                " Make sure that the device to restore to is the expected device."
            )
            + format_info_table(backup_info, device_info)
            + restore_msg
        )
    elif backup_info.fw_version != device_info.fw_version:
        restore_msg = (
            _(
                "This backup was created from a device running a different firmware version."
                " This is probably okay, but you should make sure that your device is up to date"
                " before restoring."
            )
            + format_info_table(backup_info, device_info)
            + restore_msg
        )

    if not question_dialog(
        gui,
        "Restore backup?",
        restore_msg,
        default_yes=False,
        yes_text=_("Restore"),
        no_text=_("Cancel"),
    ):
        debug("Cancelled restore")
        return

    device_path = str(device.driver._main_prefix)
    debug(f"Restoring backup to {device_path}")

    # Delete the version file before restoring because we don't want to overwrite it
    # with potentially incompatible information
    (tmpdir / ".kobo/version").unlink()

    if device.is_db_copied:
        debug(f"DB is copied; restoring to {device.db_path}")

        # This import is safe since the DB can only have been copied in Calibre versions
        # where it exists
        from calibre.devices.kobo.db import kobo_db_lock

        db_tmp_path = tmpdir / ".kobo/KoboReader.sqlite"
        with kobo_db_lock:
            # If the DB has been copied to a temporary path we have to restore it to
            # both locations to ensure consistency
            shutil.copyfile(db_tmp_path, device.db_path)
            shutil.copyfile(db_tmp_path, device.device_db_path)
            for ext in ["journal", "shm", "wal"]:
                Path(f"{device.db_path}-{ext}").unlink(missing_ok=True)
                Path(f"{device.device_db_path}-{ext}").unlink(missing_ok=True)
        # Remove DB so it doesn't get copied again
        db_tmp_path.unlink()

    shutil.copytree(tmpdir, device_path, dirs_exist_ok=True)

    # Remove temporary SQLite files that could cause issues with the restored DB
    for ext in ["journal", "shm", "wal"]:
        Path(f"{device.db_path}-{ext}").unlink(missing_ok=True)

    debug("Backup successfully restored")

    info_dialog(
        gui,
        _("Backup restored"),
        _(
            "Backup restored."
            " You should eject and restart your device now"
            " to ensure that the device reads the database correctly."
        ),
        show=True,
        show_copy_button=False,
    )


def invalid_backup_dialog(filename: str, gui: ui.Main) -> None:
    error_dialog(
        gui,
        _("Invalid backup"),
        _(
            'The file "{0}" does not appear to be a valid KoboUtilities backup file'
        ).format(filename),
        show=True,
    )


def get_backup_info(
    tmpdir: Path, bk_device_name: str, device: KoboDevice
) -> tuple[BackupInfo, DeviceInfo]:
    bk_version_info = (tmpdir / ".kobo/version").read_text().split(",")
    bk_serial_number = bk_version_info[0]
    bk_fwversion = bk_version_info[2]

    dv_serial_number = device.version_info.serial_no
    dv_device_name = "".join(device.driver.gui_name.split())
    dv_fwversion = ".".join(map(str, device.driver.fwversion))

    return (
        BackupInfo(BackupDeviceInfo(bk_device_name, bk_serial_number, bk_fwversion)),
        DeviceInfo(BackupDeviceInfo(dv_device_name, dv_serial_number, dv_fwversion)),
    )


def format_info_table(backup_info: BackupInfo, device_info: DeviceInfo) -> str:
    row = "<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>"

    def format_info_row(name: str, v1: str, v2: str) -> str:
        if v1 == v2:
            return row.format(name, v1, v2)
        return row.format(
            name,
            f"<span style='color: red; font-weight: bold'>{v1}</span>",
            f"<span style='color: red; font-weight: bold'>{v2}</span>",
        )

    table = "<p><table>"
    table += "<tr><th></th><th>{0}</th><th>{1}</th></tr>".format(
        _("Backup"), _("Device")
    )
    table += format_info_row(
        _("Device type"), backup_info.device_name, device_info.device_name
    )
    table += format_info_row(
        _("Serial number"), backup_info.serial_number, device_info.serial_number
    )
    table += format_info_row(
        _("Firmware version"), backup_info.fw_version, device_info.fw_version
    )
    table += "</table><p>"

    return table
