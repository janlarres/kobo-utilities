from __future__ import annotations

import os
from typing import TYPE_CHECKING, cast

from calibre.devices.kobo.driver import KOBOTOUCH
from calibre.gui2 import info_dialog, open_local_file
from calibre.gui2.widgets2 import ColorButton
from qt.core import QCheckBox, QDialogButtonBox, QGridLayout, QGroupBox, QVBoxLayout

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


def upload_covers(
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
    debug("selectedIDs:", selectedIDs)
    books = utils.convert_calibre_ids_to_books(
        current_view.model().db, selectedIDs, get_cover=True
    )

    dlg = CoverUploadOptionsDialog(gui, device)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    options = cfg.plugin_prefs.coverUpload
    total_books, uploaded_covers, not_on_device_books = _upload_covers(
        books, device, gui, options
    )
    result_message = (
        _("Change summary:")
        + "\n\t"
        + _("Covers uploaded={0}\n\tBooks not on device={1}\n\tTotal books={2}").format(
            uploaded_covers, not_on_device_books, total_books
        )
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Covers uploaded"),
        result_message,
        show=True,
    )


def remove_covers(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return
    debug("device.path", device.path)

    if gui.stack.currentIndex() == 0:
        selectedIDs = utils.get_selected_ids(gui)
        books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)
    else:
        books = utils.get_books_for_selected(gui)

    if len(books) == 0:
        return

    dlg = RemoveCoverOptionsDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    options = cfg.plugin_prefs.removeCovers
    removed_covers, not_on_device_books, total_books = _remove_covers(
        books, device, gui, options
    )
    result_message = (
        _("Change summary:")
        + "\n\t"
        + _("Covers removed={0}\n\tBooks not on device={1}\n\tTotal books={2}").format(
            removed_covers, not_on_device_books, total_books
        )
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Covers removed"),
        result_message,
        show=True,
    )


def open_cover_image_directory(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None:
        return
    debug("device.path", device.path)

    if gui.stack.currentIndex() == 0:
        selectedIDs = utils.get_selected_ids(gui)
        books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)

    else:
        books = utils.get_books_for_selected(gui)

    if len(books) == 0:
        return

    _open_cover_image_directory(books, device, gui)


def _upload_covers(
    books: list[Book], device: KoboDevice, gui: ui.Main, options: cfg.CoverUploadConfig
):
    uploaded_covers = 0
    total_books = 0
    not_on_device_books = len(books)

    kobo_kepub_dir = cast("str", device.driver.normalize_path(".kobo/kepub/"))
    sd_kepub_dir = cast("str", device.driver.normalize_path("koboExtStorage/kepub/"))
    debug("kobo_kepub_dir=", kobo_kepub_dir)
    # Extra cover upload options were added in calibre 3.45.
    driver_supports_extended_cover_options = hasattr(device, "dithered_covers")
    driver_supports_cover_letterbox_colors = hasattr(
        device, "letterbox_fs_covers_color"
    )

    for book in books:
        total_books += 1
        paths = utils.get_device_paths_from_id(cast("int", book.calibre_id), gui)
        not_on_device_books -= 1 if len(paths) > 0 else 0
        for path in paths:
            debug("path=", path)
            if (
                kobo_kepub_dir not in path and sd_kepub_dir not in path
            ) or options.kepub_covers:
                if isinstance(device.driver, KOBOTOUCH):
                    if driver_supports_cover_letterbox_colors:
                        device.driver._upload_cover(
                            path,
                            "",
                            book,
                            path,
                            options.blackandwhite,
                            dithered_covers=options.dithered_covers,
                            keep_cover_aspect=options.keep_cover_aspect,
                            letterbox_fs_covers=options.letterbox,
                            letterbox_color=options.letterbox_color,
                            png_covers=options.png_covers,
                        )
                    elif driver_supports_extended_cover_options:
                        device.driver._upload_cover(
                            path,
                            "",
                            book,
                            path,
                            options.blackandwhite,
                            dithered_covers=options.dithered_covers,
                            keep_cover_aspect=options.keep_cover_aspect,
                            letterbox_fs_covers=options.letterbox,
                            png_covers=options.png_covers,
                        )
                    else:
                        device.driver._upload_cover(
                            path,
                            "",
                            book,
                            path,
                            options.blackandwhite,
                            keep_cover_aspect=options.keep_cover_aspect,
                        )
                else:
                    device.driver._upload_cover(
                        path,
                        "",
                        book,
                        path,
                        options.blackandwhite,
                    )
                uploaded_covers += 1

    return total_books, uploaded_covers, not_on_device_books


def _remove_covers(
    books: list[Book], device: KoboDevice, gui: ui.Main, options: cfg.RemoveCoversConfig
):
    connection = utils.device_database_connection(device)
    total_books = 0
    removed_covers = 0
    not_on_device_books = 0

    # These should have been checked in the calling method
    assert device is not None
    assert isinstance(device.driver, KOBOTOUCH)

    remove_fullsize_covers = options.remove_fullsize_covers
    debug("remove_fullsize_covers=", remove_fullsize_covers)

    imageId_query = (
        "SELECT ImageId "
        "FROM content "
        "WHERE ContentType = ? "
        "AND ContentId = ?"
    )  # fmt: skip
    cursor = connection.cursor()

    for book in books:
        debug("book=", book)
        debug("book.__class__=", book.__class__)
        debug("book.contentID=", book.contentID)
        debug("book.lpath=", book.lpath)
        debug("book.path=", book.path)
        contentIDs = (
            [book.contentID]
            if book.contentID is not None
            else utils.get_contentIDs_from_id(cast("int", book.calibre_id), gui)
        )
        debug("contentIDs=", contentIDs)
        for contentID in contentIDs:
            debug("contentID=", contentID)
            if not contentID or (
                "file:///" not in contentID and not options.kepub_covers
            ):
                continue

            if contentID.startswith("file:///mnt/sd/"):
                path = device.driver._card_a_prefix
            else:
                path = device.driver._main_prefix

            query_values = (
                BOOK_CONTENTTYPE,
                contentID,
            )
            cursor.execute(imageId_query, query_values)
            try:
                result = next(cursor)
                debug("contentId='%s', imageId='%s'" % (contentID, result[0]))
                image_id = result[0]
                debug("image_id=", image_id)
                if image_id is not None:
                    image_path = device.driver.images_path(path, image_id)
                    debug("image_path=%s" % image_path)

                    for ending in list(device.driver.cover_file_endings().keys()):
                        debug("ending='%s'" % ending)
                        if remove_fullsize_covers and ending != " - N3_FULL.parsed":
                            debug("not the full sized cover. Skipping")
                            continue
                        fpath = image_path + ending
                        fpath = device.driver.normalize_path(fpath)
                        assert isinstance(fpath, str)
                        debug("fpath=%s" % fpath)

                        if os.path.exists(fpath):
                            debug("Image File Exists")
                            os.unlink(fpath)

                    try:
                        os.removedirs(os.path.dirname(image_path))
                    except Exception as e:
                        debug(
                            "unable to remove dir '%s': %s"
                            % (os.path.dirname(image_path), e)
                        )
                removed_covers += 1
            except StopIteration:
                debug("no match for contentId='%s'" % (contentID,))
                not_on_device_books += 1
            total_books += 1

    return removed_covers, not_on_device_books, total_books


def _open_cover_image_directory(books: list[Book], device: KoboDevice, gui: ui.Main):
    connection = utils.device_database_connection(device, use_row_factory=True)
    total_books = 0
    removed_covers = 0
    not_on_device_books = 0

    assert isinstance(device.driver, KOBOTOUCH)

    imageId_query = (
        "SELECT ImageId "
        "FROM content "
        "WHERE ContentType = ? "
        "AND ContentId = ?"
    )  # fmt: skip
    cursor = connection.cursor()

    for book in books:
        debug("book=", book)
        debug("book.__class__=", book.__class__)
        debug("book.contentID=", book.contentID)
        debug("book.lpath=", book.lpath)
        debug("book.path=", book.path)
        contentIDs = (
            [book.contentID]
            if book.contentID is not None
            else utils.get_contentIDs_from_id(cast("int", book.calibre_id), gui)
        )
        debug("contentIDs=", contentIDs)
        for contentID in contentIDs:
            debug("contentID=", contentID)

            if contentID is None:
                debug("Book does not have a content id.")
                continue
            if contentID.startswith("file:///mnt/sd/"):
                path = device.driver._card_a_prefix
            else:
                path = device.driver._main_prefix

            query_values = (
                BOOK_CONTENTTYPE,
                contentID,
            )
            cursor.execute(imageId_query, query_values)
            image_id = None
            try:
                result = next(cursor)
                debug("contentId='%s', imageId='%s'" % (contentID, result["ImageId"]))
                image_id = result["ImageId"]
            except StopIteration:
                debug("no match for contentId='%s'" % (contentID,))
                image_id = device.driver.imageid_from_contentid(contentID)

            if image_id:
                cover_image_file = device.driver.images_path(path, image_id)
                debug("cover_image_file='%s'" % (cover_image_file))
                cover_dir = os.path.dirname(os.path.abspath(cover_image_file))
                debug("cover_dir='%s'" % (cover_dir))
                if os.path.exists(cover_dir):
                    open_local_file(cover_dir)
            total_books += 1

    return removed_covers, not_on_device_books, total_books


class CoverUploadOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget, device: KoboDevice):
        super().__init__(
            parent,
            "kobo utilities plugin:cover upload settings dialog",
        )
        self.initialize_controls()

        options = cfg.plugin_prefs.coverUpload

        # Set some default values from last time dialog was used.
        blackandwhite = options.blackandwhite
        self.blackandwhite_checkbox.setChecked(blackandwhite)
        self.blackandwhite_checkbox_clicked(blackandwhite)
        self.ditheredcovers_checkbox.setChecked(options.dithered_covers)

        # Hide options if the driver doesn't have the extended options.
        self.driver_supports_extended_cover_options = hasattr(
            device.driver, "dithered_covers"
        )
        self.driver_supports_cover_letterbox_colors = hasattr(
            device.driver, "letterbox_fs_covers_color"
        )
        self.ditheredcovers_checkbox.setVisible(
            self.driver_supports_extended_cover_options
        )
        self.letterbox_checkbox.setVisible(self.driver_supports_extended_cover_options)
        self.pngcovers_checkbox.setVisible(self.driver_supports_extended_cover_options)
        self.letterbox_colorbutton.setVisible(
            self.driver_supports_cover_letterbox_colors
        )

        letterbox = options.letterbox
        self.letterbox_checkbox.setChecked(letterbox)
        self.letterbox_checkbox_clicked(letterbox)
        keep_cover_aspect = options.keep_cover_aspect
        self.keep_cover_aspect_checkbox.setChecked(keep_cover_aspect)
        self.keep_cover_aspect_checkbox_clicked(keep_cover_aspect)
        letterbox_color = options.letterbox_color
        self.letterbox_colorbutton.color = letterbox_color
        self.pngcovers_checkbox.setChecked(options.png_covers)
        self.kepub_covers_checkbox.setChecked(options.kepub_covers)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "default_cover.png",
            _("Upload covers"),
            "UploadCovers",
        )
        layout.addLayout(title_layout, stretch=0)

        options_group = QGroupBox(_("Upload covers"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        self.blackandwhite_checkbox = QCheckBox(_("Black and white covers"), self)
        options_layout.addWidget(self.blackandwhite_checkbox, 0, 0, 1, 1)
        self.blackandwhite_checkbox.clicked.connect(self.blackandwhite_checkbox_clicked)
        self.ditheredcovers_checkbox = QCheckBox(_("Dithered covers"), self)
        options_layout.addWidget(self.ditheredcovers_checkbox, 0, 1, 1, 1)
        self.pngcovers_checkbox = QCheckBox(_("PNG covers"), self)
        options_layout.addWidget(self.pngcovers_checkbox, 0, 2, 1, 2)

        self.keep_cover_aspect_checkbox = QCheckBox(_("Keep cover aspect ratio"), self)
        options_layout.addWidget(self.keep_cover_aspect_checkbox, 1, 0, 1, 1)
        self.keep_cover_aspect_checkbox.clicked.connect(
            self.keep_cover_aspect_checkbox_clicked
        )
        self.letterbox_checkbox = QCheckBox(_("Letterbox covers"), self)
        options_layout.addWidget(self.letterbox_checkbox, 1, 1, 1, 1)
        self.letterbox_checkbox.clicked.connect(self.letterbox_checkbox_clicked)

        self.letterbox_colorbutton = ColorButton(options_layout)
        self.letterbox_colorbutton.setToolTip(
            _(
                "Choose the color to use when letterboxing the cover."
                " The default color is black (#000000)"
            )
        )
        options_layout.addWidget(self.letterbox_colorbutton, 1, 2, 1, 1)

        self.kepub_covers_checkbox = QCheckBox(_("Upload covers for Kobo ePubs"), self)
        options_layout.addWidget(self.kepub_covers_checkbox, 2, 0, 1, 3)
        options_layout.setColumnStretch(0, 0)
        options_layout.setColumnStretch(1, 0)
        options_layout.setColumnStretch(2, 0)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self):
        with cfg.plugin_prefs.coverUpload as options:
            options.blackandwhite = self.blackandwhite_checkbox.isChecked()
            options.dithered_covers = self.ditheredcovers_checkbox.isChecked()
            options.png_covers = self.pngcovers_checkbox.isChecked()
            options.keep_cover_aspect = self.keep_cover_aspect_checkbox.isChecked()
            options.letterbox = self.letterbox_checkbox.isChecked()
            if self.driver_supports_cover_letterbox_colors:
                options.letterbox_color = cast("str", self.letterbox_colorbutton.color)
            options.kepub_covers = self.kepub_covers_checkbox.isChecked()

        self.accept()

    def blackandwhite_checkbox_clicked(self, checked: bool):
        self.ditheredcovers_checkbox.setEnabled(
            checked and self.blackandwhite_checkbox.isChecked()
        )
        self.pngcovers_checkbox.setEnabled(
            checked and self.blackandwhite_checkbox.isChecked()
        )

    def keep_cover_aspect_checkbox_clicked(self, checked: bool):
        self.letterbox_checkbox.setEnabled(
            checked and self.keep_cover_aspect_checkbox.isChecked()
        )
        self.letterbox_colorbutton.setEnabled(
            checked and self.letterbox_checkbox.isChecked()
        )

    def letterbox_checkbox_clicked(self, checked: bool):
        self.letterbox_colorbutton.setEnabled(
            checked and self.letterbox_checkbox.isChecked()
        )


class RemoveCoverOptionsDialog(PluginDialog):
    def __init__(self, parent: QWidget):
        super().__init__(
            parent,
            "kobo utilities plugin:remove cover settings dialog",
        )
        self.initialize_controls()

        options = cfg.plugin_prefs.removeCovers
        self.remove_fullsize_covers_checkbox.setChecked(options.remove_fullsize_covers)
        self.kepub_covers_checkbox.setChecked(options.kepub_covers)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "default_cover.png",
            _("Remove covers"),
            "RemoveCovers",
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("Remove covers"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        self.remove_fullsize_covers_checkbox = QCheckBox(
            _("Remove full size covers"), self
        )
        self.remove_fullsize_covers_checkbox.setToolTip(
            _(
                "Check this if you want to remove just the full size cover from the device. This will save space, but, if covers are used for the sleep screen, they will not look very good."
            )
        )
        options_layout.addWidget(self.remove_fullsize_covers_checkbox, 0, 0, 1, 1)

        self.kepub_covers_checkbox = QCheckBox(_("Remove covers for Kobo epubs"), self)
        self.kepub_covers_checkbox.setToolTip(
            _(
                "Check this if you want to remove covers for any Kobo epubs synced from the Kobo server."
            )
        )
        options_layout.addWidget(self.kepub_covers_checkbox, 2, 0, 1, 1)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self):
        with cfg.plugin_prefs.removeCovers as options:
            options.remove_fullsize_covers = (
                self.remove_fullsize_covers_checkbox.isChecked()
            )
            options.kepub_covers = self.kepub_covers_checkbox.isChecked()
        self.accept()
