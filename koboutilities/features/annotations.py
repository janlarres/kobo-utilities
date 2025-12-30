from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING, cast

from calibre.ebooks.metadata import authors_to_string
from calibre.gui2 import choose_dir, error_dialog, info_dialog
from calibre.gui2.dialogs.message_box import ViewLog
from qt.core import (
    QDialogButtonBox,
    QGridLayout,
    QLabel,
    QLineEdit,
    QModelIndex,
    QPushButton,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog
from ..utils import debug

if TYPE_CHECKING:
    from calibre.devices.kobo.books import Book
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def getAnnotationForSelected(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    _getAnnotationForSelected(device, gui)


def _getAnnotationForSelected(device: KoboDevice, gui: ui.Main) -> None:
    # Generate a path_map from selected ids
    def get_ids_from_selected_rows() -> list[int]:
        rows = gui.library_view.selectionModel().selectedRows()
        if not rows or len(rows) < 1:
            rows = range(gui.library_view.model().rowCount(QModelIndex()))
        return list(map(gui.library_view.model().id, rows))

    def get_formats(_id: int) -> list[str]:
        formats = db.formats(_id, index_is_id=True)
        return [fmt.lower() for fmt in formats.split(",")]

    def generate_annotation_paths(
        ids: list[int],
    ) -> dict[int, dict[str, str | list[str]]]:
        # Generate path templates
        # Individual storage mount points scanned/resolved in driver.get_annotations()
        path_map = {}
        for _id in ids:
            paths = utils.get_device_paths_from_id(_id, gui)
            debug("paths=", paths)
            if len(paths) > 0:
                the_path = paths[0]
                if len(paths) > 1 and (
                    len(os.path.splitext(paths[0])) > 1
                ):  # No extension - is kepub
                    the_path = paths[1]
                path_map[_id] = {"path": the_path, "fmts": get_formats(_id)}
        return path_map

    annotationText = []

    if gui.current_view() is not gui.library_view:
        error_dialog(
            gui,
            _("Use library only"),
            _("User annotations generated from main library only"),
            show=True,
        )
        return
    db = gui.library_view.model().db

    # Get the list of ids
    ids = get_ids_from_selected_rows()
    if not ids:
        error_dialog(
            gui,
            _("No books selected"),
            _("No books selected to fetch annotations from"),
            show=True,
        )
        return

    debug("ids=", ids)
    # Map ids to paths
    path_map = generate_annotation_paths(ids)
    debug("path_map=", path_map)
    if len(path_map) == 0:
        error_dialog(
            gui,
            _("No books on device selected"),
            _(
                "None of the books selected were on the device. Annotations can only be copied for books on the device."
            ),
            show=True,
        )
        return

    # Dispatch to the device get_annotations()
    debug("path_map=", path_map)
    bookmarked_books = device.driver.get_annotations(path_map)
    debug("bookmarked_books=", bookmarked_books)

    for id_ in bookmarked_books:
        bm = device.driver.UserAnnotation(
            bookmarked_books[id_][0], bookmarked_books[id_][1]
        )

        mi = db.get_metadata(id_, index_is_id=True)

        user_notes_soup = device.driver.generate_annotation_html(bm.value)
        book_heading = "<b>%(title)s</b> by <b>%(author)s</b>" % {
            "title": mi.title,
            "author": authors_to_string(mi.authors),
        }
        bookmark_html = str(user_notes_soup.div)
        debug("bookmark_html:", bookmark_html)
        annotationText.append(book_heading + bookmark_html)

    d = ViewLog("Kobo Touch Annotation", "\n<hr/>\n".join(annotationText), parent=gui)
    d.exec()


def backup_annotation_files(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    selectedIDs = utils.get_selected_ids(gui)

    if len(selectedIDs) == 0:
        return

    dlg = BackupAnnotationsOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    dest_path = dlg.dest_path()
    debug("selectedIDs:", selectedIDs)
    books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)
    for book in books:
        device_book_paths = utils.get_device_paths_from_id(
            cast("int", book.calibre_id), gui
        )
        debug("device_book_paths:", device_book_paths)
        book.paths = device_book_paths
        book.contentIDs = [
            utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
            for path in device_book_paths
        ]

    debug("dest_path=", dest_path)
    annotations_found, no_annotations, kepubs, count_books = _backup_annotation_files(
        device, books, dest_path
    )
    result_message = _(
        "Annotations backup summary:\n\tBooks with annotations={0}\n\tBooks without annotations={1}\n\tKobo epubs={2}\n\tTotal books={3}"
    ).format(annotations_found, no_annotations, kepubs, count_books)
    info_dialog(
        gui,
        _("Kobo Utilities") + _(" - Annotations backup"),
        result_message,
        show=True,
    )


def _backup_annotation_files(device: KoboDevice, books: list[Book], dest_path: str):
    annotations_found = 0
    kepubs = 0
    no_annotations = 0
    count_books = 0

    debug("self.device.path='%s'" % (device.path))
    kepub_dir = cast("str", device.driver.normalize_path(".kobo/kepub/"))
    annotations_dir = cast(
        "str",
        device.driver.normalize_path(device.path + "Digital Editions/Annotations/"),
    )
    annotations_ext = ".annot"

    for book in books:
        count_books += 1

        for book_path in cast("list[str]", book.paths):
            relative_path = book_path.replace(device.path, "")
            annotation_file = device.driver.normalize_path(
                annotations_dir + relative_path + annotations_ext
            )
            assert annotation_file is not None
            debug(
                "kepub title='%s' annotation_file='%s'" % (book.title, annotation_file)
            )
            if relative_path.startswith(kepub_dir):
                debug("kepub title='%s' book_path='%s'" % (book.title, book_path))
                kepubs += 1
            elif os.path.exists(annotation_file):
                debug("book_path='%s'" % (book_path))
                backup_file = device.driver.normalize_path(
                    dest_path + "/" + relative_path + annotations_ext
                )
                assert backup_file is not None
                debug("backup_file='%s'" % (backup_file))
                d, p = os.path.splitdrive(backup_file)
                debug("d='%s' p='%s'" % (d, p))
                backup_path = os.path.dirname(str(backup_file))
                try:
                    os.makedirs(backup_path)
                except OSError:
                    debug("path exists: backup_path='%s'" % (backup_path))
                shutil.copyfile(annotation_file, backup_file)
                annotations_found += 1
            else:
                debug("book_path='%s'" % (book_path))
                no_annotations += 1

    debug(
        "Backup summary: annotations_found=%d, no_annotations=%d, kepubs=%d Total=%d"
        % (annotations_found, no_annotations, kepubs, count_books)
    )

    return (annotations_found, no_annotations, kepubs, count_books)


class BackupAnnotationsOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget):
        super().__init__(
            parent,
            "kobo utilities plugin:backup annotation files settings dialog",
        )
        self.initialize_controls()

        self.dest_directory_edit.setText(
            cfg.plugin_prefs.backupAnnotations.dest_directory
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
            _("Back up annotations files"),
            "BackupAnnotations",
        )
        layout.addLayout(title_layout)
        options_layout = QGridLayout()
        layout.addLayout(options_layout)

        dest_directory_label = QLabel(_("Destination:"), self)
        dest_directory_label.setToolTip(
            _("Select the destination the annotations files are to be backed up in.")
        )
        self.dest_directory_edit = QLineEdit(self)
        self.dest_directory_edit.setMinimumSize(200, 0)
        dest_directory_label.setBuddy(self.dest_directory_edit)
        dest_pick_button = QPushButton(_("..."), self)
        dest_pick_button.setMaximumSize(24, 20)
        dest_pick_button.clicked.connect(self._get_dest_directory_name)
        options_layout.addWidget(dest_directory_label, 0, 0, 1, 1)
        options_layout.addWidget(self.dest_directory_edit, 0, 1, 1, 1)
        options_layout.addWidget(dest_pick_button, 0, 2, 1, 1)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self) -> None:
        if len(self.dest_directory_edit.text()) == 0:
            error_dialog(
                self,
                "No destination",
                "You must enter a destination directory to save the annotation files in",
                show=True,
                show_copy_button=False,
            )

        cfg.plugin_prefs.backupAnnotations.dest_directory = (
            self.dest_directory_edit.text()
        )
        self.accept()

    def dest_path(self):
        return self.dest_directory_edit.text()

    def _get_dest_directory_name(self):
        path = choose_dir(
            self,
            "back up annotations destination dialog",
            _("Choose destination directory"),
        )
        self.dest_directory_edit.setText(path)
