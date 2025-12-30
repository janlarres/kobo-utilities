from __future__ import annotations

from typing import TYPE_CHECKING, cast

from calibre.gui2 import error_dialog, info_dialog, question_dialog
from qt.core import (
    QCheckBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QIcon,
    QLabel,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME
from ..dialogs import (
    CustomColumnComboBox,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
)
from ..utils import debug

if TYPE_CHECKING:
    from calibre.devices.kobo.books import Book
    from calibre.gui2 import ui

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources


def get_shelves_from_device(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None:
        return

    debug("start")

    dlg = GetShelvesFromDeviceDialog(gui)
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        debug("dialog cancelled")
        return

    shelves_column = cfg.get_library_config(gui.current_db).shelvesColumn

    # Check if driver is configured to manage shelves. If so, warn if selected column is one of
    # the configured columns.
    driver_shelves = device.driver.get_collections_attributes()
    debug("driver_shelves=", driver_shelves)
    debug("selected column=", shelves_column)
    if shelves_column in driver_shelves:
        debug("selected column is one of the columns used in the driver configuration!")
        details_msg = _(
            "The selected column is {0}."
            "\n"
            "The driver collection management columns are: {1}"
        ).format(shelves_column, ", ".join(driver_shelves))
        mb = question_dialog(
            gui,
            _("Getting collections from device"),
            _(
                "The column selected is one of the columns used in the driver configuration for collection management. "
                "Updating this column might affect the collection management the next time you connect the device. "
                "\n\nAre you sure you want to do this?"
            ),
            override_icon=QIcon(I("dialog_warning.png")),
            show_copy_button=False,
            det_msg=details_msg,
        )
        if not mb:
            debug("User cancelled because of column used.")
            return

    progressbar = ProgressBar(
        parent=gui, window_title=_("Getting collections from device")
    )
    progressbar.show()
    progressbar.set_label(_("Getting list of collections"))

    library_db = current_view.model().db
    options = cfg.plugin_prefs.getShelvesOptionStore
    if options.allBooks:
        selectedIDs = set(
            library_db.search_getting_ids(
                "ondevice:True", None, sort_results=False, use_virtual_library=False
            )
        )
    else:
        selectedIDs = utils.get_selected_ids(gui)

    if len(selectedIDs) == 0:
        return
    debug("selectedIDs:", selectedIDs)
    books = utils.convert_calibre_ids_to_books(library_db, selectedIDs)
    progressbar.set_label(
        _("Number of books to get collections for: {0}").format(len(books))
    )
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

    debug("about get shelves - options=%s" % options)

    books_with_shelves, books_without_shelves, count_books = _get_shelves_from_device(
        books, options, device, gui, progressbar
    )
    result_message = (
        _("Update summary:")
        + "\n\t"
        + _(
            "Books processed={0}\n\tBooks with collections={1}\n\tBooks without collections={2}"
        ).format(count_books, books_with_shelves, books_without_shelves)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Get collections from device"),
        result_message,
        show=True,
    )


def _get_shelves_from_device(
    books: list[Book],
    options: cfg.GetShelvesOptionStoreConfig,
    device: KoboDevice,
    gui: ui.Main,
    progressbar: ProgressBar,
):
    count_books = 0
    books_with_shelves = 0
    books_without_shelves = 0
    replace_shelves = options.replaceShelves

    total_books = len(books)
    progressbar.show_with_maximum(total_books)

    fetch_query = (
        "SELECT c.ContentID, sc.ShelfName "
        "FROM content c LEFT OUTER JOIN ShelfContent sc "
        "ON c.ContentID = sc.ContentId AND c.ContentType = 6  AND sc._IsDeleted = 'false' "
        "JOIN Shelf s ON s.Name = sc.ShelfName AND s._IsDeleted = 'false' "
        "WHERE c.ContentID = ? "
        "ORDER BY c.ContentID, sc.ShelfName"
    )

    connection = utils.device_database_connection(device)
    library_db = gui.current_db
    library_config = cfg.get_library_config(library_db)
    bookshelf_column_name = library_config.shelvesColumn
    debug("bookshelf_column_name=", bookshelf_column_name)
    bookshelf_column = library_db.field_metadata[bookshelf_column_name]
    bookshelf_column_label = library_db.field_metadata.key_to_label(
        bookshelf_column_name
    )
    bookshelf_column_is_multiple = (
        bookshelf_column["is_multiple"] is not None
        and len(bookshelf_column["is_multiple"]) > 0
    )
    debug("bookshelf_column_label=", bookshelf_column_label)
    debug("bookshelf_column_is_multiple=", bookshelf_column_is_multiple)

    cursor = connection.cursor()
    for book in books:
        progressbar.set_label(_("Getting collections for {}").format(book.title))
        progressbar.increment()
        count_books += 1
        shelf_names = []
        update_library = False
        for contentID in cast("list[str]", book.contentIDs):
            debug("title='%s' contentId='%s'" % (book.title, contentID))
            fetch_values = (contentID,)
            debug("tetch_query='%s'" % (fetch_query))
            cursor.execute(fetch_query, fetch_values)

            for row in cursor:
                debug("result=", row)
                shelf_names.append(row[1])
                update_library = True

        if len(shelf_names) > 0:
            books_with_shelves += 1
        else:
            books_without_shelves += 1
            continue

        if update_library and len(shelf_names) > 0:
            debug("device shelf_names='%s'" % (shelf_names))
            debug("device set(shelf_names)='%s'" % (set(shelf_names)))
            metadata = book.get_user_metadata(bookshelf_column_name, True)
            assert metadata is not None
            old_value = metadata["#value#"]
            debug("library shelf names='%s'" % (old_value))
            if old_value is None or set(old_value) != set(shelf_names):
                debug("shelves are not the same")
                shelf_names = (
                    list(set(shelf_names))
                    if bookshelf_column_is_multiple
                    else ", ".join(shelf_names)
                )
                debug("device shelf_names='%s'" % (shelf_names))
                if replace_shelves or old_value is None:
                    new_value = shelf_names
                elif bookshelf_column_is_multiple:
                    new_value = old_value + shelf_names
                else:
                    new_value = old_value + ", " + shelf_names
                debug("new shelf names='%s'" % (new_value))
                library_db.set_custom(
                    book.calibre_id,
                    new_value,
                    label=bookshelf_column_label,
                    commit=False,
                )

        else:
            books_with_shelves -= 1
            books_without_shelves += 1

    library_db.commit()
    progressbar.hide()

    return (books_with_shelves, books_without_shelves, count_books)


class GetShelvesFromDeviceDialog(PluginDialog):
    def __init__(self, parent: ui.Main):
        super().__init__(
            parent,
            "kobo utilities plugin:get shelves from device settings dialog",
        )
        self.initialize_controls()
        self.gui = parent

        all_books = cfg.plugin_prefs.getShelvesOptionStore.allBooks
        self.all_books_checkbox.setChecked(all_books)

        replace_shelves = cfg.plugin_prefs.getShelvesOptionStore.replaceShelves
        self.replace_shelves_checkbox.setChecked(replace_shelves)

        self.library_config = cfg.get_library_config(parent.current_db)
        shelf_column = self.library_config.shelvesColumn
        self.tag_type_custom_columns = self.get_tag_type_custom_columns()
        self.shelf_column_combo.populate_combo(
            self.tag_type_custom_columns, shelf_column
        )
        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self, "images/icon.png", _("Get collections from device")
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("Options"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        shelf_column_label = QLabel(_("Collection column:"), self)
        shelf_column_tooltip = _(
            "Select a custom column to store the retrieved collection names. The column type must\nbe of type 'text'."
        )
        shelf_column_label.setToolTip(shelf_column_tooltip)
        self.shelf_column_combo = CustomColumnComboBox(self)
        self.shelf_column_combo.setToolTip(shelf_column_tooltip)
        shelf_column_label.setBuddy(self.shelf_column_combo)
        options_layout.addWidget(shelf_column_label, 0, 0, 1, 1)
        options_layout.addWidget(self.shelf_column_combo, 0, 1, 1, 1)

        self.all_books_checkbox = QCheckBox(_("All books on device"), self)
        self.all_books_checkbox.setToolTip(
            _(
                "Get the collections for all the books on the device that are in the library. If not checked, will only get them for the selected books."
            )
        )
        options_layout.addWidget(self.all_books_checkbox, 1, 0, 1, 2)

        self.replace_shelves_checkbox = QCheckBox(
            _("Replace column with collections"), self
        )
        self.replace_shelves_checkbox.setToolTip(
            _(
                "If this is selected, the current value in the library, will be replaced by\nthe retrieved collections. Otherwise, the retrieved collections will be added to the value"
            )
        )
        options_layout.addWidget(self.replace_shelves_checkbox, 2, 0, 1, 2)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_tag_type_custom_columns(self):
        column_types = ["text"]
        return self.get_custom_columns(column_types)

    def get_custom_columns(self, column_types: list[str]) -> dict[str, str]:
        custom_columns = self.gui.library_view.model().custom_columns
        available_columns: dict[str, str] = {}
        for key, column in custom_columns.items():
            typ = column["datatype"]
            if typ in column_types:
                available_columns[key] = column["name"]
        return available_columns

    def ok_clicked(self) -> None:
        with cfg.plugin_prefs.getShelvesOptionStore as options:
            options.allBooks = self.all_books_checkbox.isChecked()
            options.replaceShelves = self.replace_shelves_checkbox.isChecked()

        shelves_column = self.shelf_column_combo.get_selected_column()
        if not shelves_column:
            error_dialog(
                self,
                _("No collection column selected"),
                _(
                    "You must select a column to populate from the collections on the device"
                ),
                show=True,
                show_copy_button=False,
            )
            return

        self.library_config.shelvesColumn = shelves_column
        cfg.set_library_config(self.gui.current_db, self.library_config)

        self.accept()
