from __future__ import annotations

import os
import pickle
import shutil
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, cast

from calibre.ebooks.BeautifulSoup import BeautifulStoneSoup
from calibre.ebooks.metadata import authors_to_string
from calibre.gui2 import info_dialog
from qt.core import (
    QButtonGroup,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QProgressDialog,
    QRadioButton,
    QTimer,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog
from ..utils import debug

if TYPE_CHECKING:
    from calibre.db.legacy import LibraryDatabase
    from calibre.gui2 import ui
    from calibre.gui2.device import DeviceJob
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources

load_translations()


@dataclass
class RemoveAnnotationsJobOptions:
    annotations_dir: str
    annotations_ext: str
    device_path: str
    remove_annot_action: cfg.RemoveAnnotationsAction


def remove_annotations_files(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del load_resources
    current_view = gui.current_view()
    if current_view is None:
        return

    dlg = RemoveAnnotationsOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    debug("device.path='%s'" % (device.path))

    options = RemoveAnnotationsJobOptions(
        str(
            device.driver.normalize_path(device.path + "Digital Editions/Annotations/")
        ),
        ".annot",
        device.path,
        cfg.plugin_prefs.removeAnnotations.removeAnnotAction,
    )

    debug("options=", options)
    RemoveAnnotationsProgressDialog(
        device,
        gui,
        dispatcher,
        options,
        current_view.model().db,
    )

    return


def _remove_annotations_job(
    gui: ui.Main,
    dispatcher: Dispatcher,
    options: RemoveAnnotationsJobOptions,
    books: list[tuple[Any]],
):
    debug("Start")

    func = "arbitrary_n"
    cpus = gui.job_manager.server.pool_size
    args = [
        remove_annotations_job.__module__,
        remove_annotations_job.__name__,
        (pickle.dumps(options), books, cpus),
    ]
    desc = _("Removing annotations files")
    gui.job_manager.run_job(
        dispatcher(partial(_remove_annotations_completed, gui=gui)),
        func,
        args=args,
        description=desc,
    )
    gui.status_bar.show_message(_("Removing annotations files") + "...")


def _remove_annotations_completed(job: DeviceJob, gui: ui.Main) -> None:
    if job.failed:
        gui.job_exception(
            job, dialog_title=_("Failed to check cover directory on device")
        )
        return
    annotations_removed = job.result
    msg = annotations_removed["message"]
    gui.status_bar.show_message(_("Cleaning annotations completed"), 3000)

    details = ""
    if msg:
        pass
    else:
        msg = _("Kobo Utilities removed <b>{0} annotation files(s)</b>.").format(0)

    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Finished"),
        msg,
        show_copy_button=True,
        show=True,
        det_msg=details,
    )


def remove_annotations_job(
    options_raw: bytes,
    books: list[tuple[int, list[str], list[str], str, str]],
    cpus: int,
    notification: Callable[[float, str], Any] = lambda _x, y: y,
):
    del cpus
    options: RemoveAnnotationsJobOptions = pickle.loads(options_raw)  # noqa: S301
    annotations_dir = options.annotations_dir
    annotations_ext = options.annotations_ext
    device_path = options.device_path
    msg = None
    details = None
    steps = 3
    current_step = 1
    annotation_files = {}

    notification(
        current_step / steps, _("Removing annotations files") + " - " + _("Start")
    )
    debug("options:", options)
    debug("len(books):", len(books))
    debug("annotations_dir: '%s'" % (annotations_dir))
    if options.remove_annot_action == cfg.RemoveAnnotationsAction.All:
        if os.path.exists(annotations_dir):
            debug("removing annotations directory")
            shutil.rmtree(annotations_dir)
            msg = _("Annotations directory removed.")
            debug("removing annotations directory - done")
    elif options.remove_annot_action == cfg.RemoveAnnotationsAction.Selected:
        if books and len(books) > 0:
            annotation_files = _get_annotation_files_for_books(
                books, annotations_dir, annotations_ext, device_path
            )
    else:
        current_step += 1
        notification(current_step / steps, _("Getting annotations files."))
        annotation_files = _get_annotation_files(annotations_dir, annotations_ext)
        msg = _("Found {0} annotation files.").format(len(annotation_files))

    if len(annotation_files.keys()) > 0:
        annotation_test_func = None
        if options.remove_annot_action == cfg.RemoveAnnotationsAction.NotOnDevice:
            annotation_test_func = _book_file_does_not_exists
        elif options.remove_annot_action == cfg.RemoveAnnotationsAction.Empty:
            annotation_test_func = _annotation_file_is_empty
        elif options.remove_annot_action == cfg.RemoveAnnotationsAction.NotEmpty:
            annotation_test_func = _annotation_file_is_not_empty
        elif options.remove_annot_action == cfg.RemoveAnnotationsAction.Selected:
            pass
        if annotation_test_func:
            current_step += 1
            notification(current_step / steps, _("Checking annotations files."))
            annotation_files = _check_annotation_files(
                annotation_files,
                annotations_dir,
                device_path,
                annotation_test_func,
            )
            msg = _("Found {0} annotation files to be removed.").format(
                len(annotation_files)
            )

    if len(annotation_files.keys()) > 0:
        current_step += 1
        notification(current_step / steps, _("Removing annotations files"))
        debug("Removing annotations files")
        annotation_files_names = set(annotation_files.keys())
        removed_annotation_files = utils.remove_extra_files(
            annotation_files_names,
            annotation_files,
            True,
            annotations_dir,
            images_tree=True,
        )
        msg = _("{0} annotations files removed.").format(len(removed_annotation_files))

    remove_annotations_result: dict[str, Any] = {}
    remove_annotations_result["message"] = msg
    remove_annotations_result["details"] = details
    remove_annotations_result["options"] = options

    current_step = steps
    notification(
        current_step / steps, _("Removing annotations files") + " - " + _("Finished")
    )

    return remove_annotations_result


def _get_annotation_files(
    annotations_path: str, annotations_ext: str
) -> dict[str, str]:
    annotation_files = {}
    if annotations_path:
        for path, dirs, files in os.walk(annotations_path):
            debug("path=%s, dirs=%s" % (path, dirs))
            debug("files=", files)
            debug("len(files)=", len(files))
            for filename in files:
                debug("filename=", filename)
                if filename.endswith(annotations_ext):
                    annotation_files[filename] = path

    return annotation_files


def _get_annotation_files_for_books(
    books: list[tuple[int, list[str], list[str], str, str]],
    annotations_path: str,
    annotations_ext: str,
    device_path: str,
) -> dict[str, str]:
    annotation_files = {}
    debug("annotations_path=", annotations_path)
    debug("device_path=", device_path)
    for book in books:
        for filename in book[2]:
            debug("filename=", filename)
            book_filename = filename
            debug("book_filename=", book_filename)
            annotation_file_path = (
                book_filename.replace(device_path, annotations_path) + annotations_ext
            )
            debug("annotation_file_path=", annotation_file_path)
            if os.path.exists(annotation_file_path):
                annotation_filename = os.path.basename(annotation_file_path)
                debug("annotation_filename=", annotation_filename)
                path = os.path.dirname(annotation_file_path)
                debug("path=", path)
                annotation_files[annotation_filename] = path

    return annotation_files


def _check_annotation_files(
    annotation_files: dict[str, str],
    annotations_dir: str,
    device_path: str,
    annotation_test_func: Callable[[str, str, str, str], bool],
) -> dict[str, str]:
    annotation_files_to_remove = {}
    for filename in annotation_files:
        debug("filename='%s', path='%s'" % (filename, annotation_files[filename]))
        file_path = annotation_files[filename]
        if annotation_test_func(filename, file_path, annotations_dir, device_path):
            debug("annotation to be removed=", filename)
            annotation_files_to_remove[filename] = file_path

    return annotation_files_to_remove


def _book_file_does_not_exists(
    annotation_filename: str,
    annotation_path: str,
    annotations_dir: str,
    device_path: str,
) -> bool:
    book_file = os.path.splitext(annotation_filename)[0]
    book_path = annotation_path.replace(annotations_dir, device_path)
    book_file = os.path.join(book_path, book_file)
    return not os.path.exists(book_file)


def _annotation_file_is_empty(
    annotation_filename: str,
    annotation_path: str,
    annotations_dir: str,
    device_path: str,
) -> bool:
    return not _annotation_file_is_not_empty(
        annotation_filename, annotation_path, annotations_dir, device_path
    )


def _annotation_file_is_not_empty(
    annotation_filename: str,
    annotation_path: str,
    annotations_dir: str,
    device_path: str,
) -> bool:
    del annotations_dir, device_path
    debug("annotation_filename=", annotation_filename)
    annotation_filepath = os.path.join(annotation_path, annotation_filename)
    with open(annotation_filepath) as annotation_file:
        soup = BeautifulStoneSoup(annotation_file.read())
        annotation = soup.find("annotation")

    return annotation is not None


class RemoveAnnotationsProgressDialog(QProgressDialog):
    def __init__(
        self,
        device: KoboDevice,
        gui: ui.Main | None,  # TODO Can this actually be None?
        dispatcher: Dispatcher,
        options: RemoveAnnotationsJobOptions,
        db: LibraryDatabase | None,
    ):
        QProgressDialog.__init__(self, "", "", 0, 0, gui)
        debug("init")
        self.setMinimumWidth(500)
        self.device = device
        self.books = []
        self.options = options
        self.db = db
        self.gui = gui
        self.dispatcher = dispatcher
        self.books_to_scan = []

        self.setWindowTitle(_("Creating queue for removing annotations files"))
        QTimer.singleShot(0, self.do_remove_annotations_queue)
        self.exec()

    def do_remove_annotations_queue(self):
        debug("start")
        if self.gui is None:
            # There is a nasty QT bug with the timers/logic above which can
            # result in the do_queue method being called twice
            return
        if self.options.remove_annot_action == cfg.RemoveAnnotationsAction.Selected:
            library_db = self.db  # self.gui.current_db
            assert library_db is not None

            self.setLabelText(_("Preparing the list of books ..."))
            self.setValue(1)

            if utils.is_device_view(self.gui):
                self.books = utils.get_books_for_selected(self.gui)
            else:
                onDeviceIds = utils.get_selected_ids(self.gui)
                self.books = utils.convert_calibre_ids_to_books(library_db, onDeviceIds)
            self.setRange(0, len(self.books))

            for i, book in enumerate(self.books, start=1):
                if utils.is_device_view(self.gui):
                    device_book_paths = [book.path]
                    contentIDs = [book.contentID]
                else:
                    device_book_paths = utils.get_device_paths_from_id(
                        cast("int", book.calibre_id), self.gui
                    )
                    contentIDs = [
                        utils.contentid_from_path(self.device, path, BOOK_CONTENTTYPE)
                        for path in device_book_paths
                    ]
                debug("device_book_paths:", device_book_paths)
                book.paths = device_book_paths
                book.contentIDs = contentIDs
                if len(book.contentIDs):
                    title = book.title
                    self.setLabelText(_("Queueing {}").format(title))
                    authors = authors_to_string(book.authors)

                    self.books_to_scan.append(
                        (book.calibre_id, book.contentIDs, book.paths, title, authors)
                    )
                self.setValue(i)
        self.hide()

        # Queue a job to process these ePub books
        _remove_annotations_job(
            self.gui, self.dispatcher, self.options, self.books_to_scan
        )


class RemoveAnnotationsOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget):
        super().__init__(
            parent,
            "kobo utilities plugin:remove annotation files settings dialog",
        )

        self.initialize_controls()
        self.annotation_clean_option_idx = (
            cfg.plugin_prefs.removeAnnotations.removeAnnotAction
        )
        button = self.annotation_clean_option_button_group.button(
            self.annotation_clean_option_idx
        )
        assert button is not None
        button.setChecked(True)
        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Remove annotations files"),
            "RemoveAnnotations",
        )
        layout.addLayout(title_layout)
        options_layout = QGridLayout()
        layout.addLayout(options_layout)

        annotation_clean_option_group_box = QGroupBox(_("Remove..."), self)
        options_layout.addWidget(annotation_clean_option_group_box, 0, 0, 1, 1)

        annotation_clean_options = {
            cfg.RemoveAnnotationsAction.All.value: (
                _("All"),
                _("Remove the annotations directory and all files within it"),
                True,
            ),
            cfg.RemoveAnnotationsAction.Selected.value: (
                _("For selected books"),
                _("Only remove annotations files for the selected books"),
                False,
            ),
            cfg.RemoveAnnotationsAction.NotOnDevice.value: (
                _("Where book is not on device"),
                _("Remove annotations files where there is no book on the device"),
                True,
            ),
            cfg.RemoveAnnotationsAction.Empty.value: (
                _("Empty"),
                _("Remove all empty annotations files"),
                True,
            ),
            cfg.RemoveAnnotationsAction.NotEmpty.value: (
                _("Not empty"),
                _("Only remove annotations files if they contain annotations"),
                True,
            ),
        }

        annotation_clean_option_group_box_layout = QVBoxLayout()
        annotation_clean_option_group_box.setLayout(
            annotation_clean_option_group_box_layout
        )
        self.annotation_clean_option_button_group = QButtonGroup(self)
        self.annotation_clean_option_button_group.buttonClicked.connect(
            self._annotation_clean_option_radio_clicked
        )
        self.annotation_clean_option_buttons = {}
        for row, clean_option in enumerate(annotation_clean_options):
            clean_options = annotation_clean_options[clean_option]
            rdo = QRadioButton(clean_options[0], self)
            rdo.setToolTip(clean_options[1])
            self.annotation_clean_option_button_group.addButton(rdo)
            self.annotation_clean_option_button_group.setId(rdo, clean_option)
            annotation_clean_option_group_box_layout.addWidget(rdo)
            self.annotation_clean_option_buttons[rdo] = row

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self):
        cfg.plugin_prefs.removeAnnotations.removeAnnotAction = (
            cfg.RemoveAnnotationsAction(self.annotation_clean_option_idx)
        )
        self.accept()

    def _annotation_clean_option_radio_clicked(self, radioButton: QRadioButton):
        self.annotation_clean_option_idx = self.annotation_clean_option_buttons[
            radioButton
        ]
