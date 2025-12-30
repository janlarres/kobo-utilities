# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2012-2020, David Forrester <davidfor@internode.on.net>"
__docformat__ = "restructuredtext en"

from typing import TYPE_CHECKING, Any, Callable

from calibre.gui2 import Application, error_dialog
from calibre.gui2.dialogs.plugin_updater import SizePersistedDialog
from calibre.utils.date import UNDEFINED_DATE, format_date, now
from qt.core import (
    QCheckBox,
    QComboBox,
    QDateTime,
    QDialog,
    QFont,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QRadioButton,
    Qt,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from . import utils
from .utils import debug

if TYPE_CHECKING:
    import datetime as dt

    from calibre.devices.kobo.books import Book
    from qt.core import QIcon


# pulls in translation files for _() strings
load_translations()


class ImageTitleLayout(QHBoxLayout):
    """
    A reusable layout widget displaying an image followed by a title
    """

    def __init__(
        self,
        parent: QWidget,
        icon_name: str,
        title: str,
        help_anchor: str | None = None,
    ):
        super().__init__()
        self.title_image_label = QLabel(parent)
        self.update_title_icon(icon_name)
        self.addWidget(self.title_image_label)

        title_font = QFont()
        title_font.setPointSize(16)
        shelf_label = QLabel(title, parent)
        shelf_label.setFont(title_font)
        self.addWidget(shelf_label)

        help_layout = QHBoxLayout()

        help_pixmap = utils.get_pixmap("help.png")
        if help_pixmap is not None:
            help_pixmap = help_pixmap.scaled(16, 16)
            help_icon = QLabel()
            help_icon.setPixmap(help_pixmap)
            # help_icon.setAlignment(Qt.AlignmentFlag.AlignRight)
            help_layout.addWidget(help_icon)

        # Add hyperlink to a help file at the right. We will replace the correct name when it is clicked.
        help_label = QLabel(
            ('<a href="http://www.foo.com/">{0}</a>').format(_("Help")), parent
        )
        help_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.LinksAccessibleByMouse
            | Qt.TextInteractionFlag.LinksAccessibleByKeyboard
        )
        help_label.linkActivated.connect(lambda _url: utils.show_help(help_anchor))
        help_layout.addWidget(help_label)

        help_layout.setAlignment(Qt.AlignmentFlag.AlignRight)
        help_widget = QWidget()
        help_widget.setLayout(help_layout)
        self.addWidget(help_widget)

    def update_title_icon(self, icon_name: str):
        pixmap = utils.get_pixmap(icon_name)
        if pixmap is None:
            error_dialog(
                self.parent(),
                _("Restart required"),
                _(
                    "Title image not found - you must restart Calibre before using this plugin!"
                ),
                show=True,
            )
        else:
            self.title_image_label.setPixmap(pixmap)
        self.title_image_label.setMaximumSize(32, 32)
        self.title_image_label.setScaledContents(True)


class PluginDialog(SizePersistedDialog):
    def __init__(self, parent: QWidget, unique_pref_name: str):
        super().__init__(parent, unique_pref_name)
        self.setWindowIcon(utils.get_icon("images/icon.png"))


class ReadOnlyTableWidgetItem(QTableWidgetItem):
    def __init__(self, text: str | None):
        if text is None:
            text = ""
        super().__init__(text)
        self.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)


class RatingTableWidgetItem(QTableWidgetItem):
    def __init__(self, rating: int | None, is_read_only: bool = False):
        super().__init__("")
        self.setData(Qt.ItemDataRole.DisplayRole, rating)
        if is_read_only:
            self.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)


class DateTableWidgetItem(QTableWidgetItem):
    def __init__(
        self,
        date_read: dt.datetime | None,
        is_read_only: bool = False,
        default_to_today: bool = False,
        fmt: str | None = None,
    ):
        if date_read is None or (date_read == UNDEFINED_DATE and default_to_today):
            date_read = now()
        if is_read_only:
            super().__init__(format_date(date_read, fmt))
            self.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled)
            self.setData(Qt.ItemDataRole.DisplayRole, QDateTime(date_read))
        else:
            super().__init__("")
            self.setData(Qt.ItemDataRole.DisplayRole, QDateTime(date_read))


class CheckableTableWidgetItem(QTableWidgetItem):
    def __init__(self, checked: bool = False):
        super().__init__("")
        self.setFlags(
            Qt.ItemFlag.ItemIsSelectable
            | Qt.ItemFlag.ItemIsUserCheckable
            | Qt.ItemFlag.ItemIsEnabled
        )
        if checked:
            self.setCheckState(Qt.CheckState.Checked)
        else:
            self.setCheckState(Qt.CheckState.Unchecked)

    def get_boolean_value(self):
        """
        Return a boolean value indicating whether checkbox is checked
        If this is a tristate checkbox, a partially checked value is returned as None
        """
        if self.checkState() == Qt.CheckState.PartiallyChecked:
            return None
        return self.checkState() == Qt.CheckState.Checked


class ReadOnlyTextIconWidgetItem(ReadOnlyTableWidgetItem):
    def __init__(self, text: str | None, icon: QIcon):
        super().__init__(text)
        if icon:
            self.setIcon(icon)


class CustomColumnComboBox(QComboBox):
    CREATE_NEW_COLUMN_ITEM = _("Create new column")

    def __init__(
        self,
        parent: QWidget,
        custom_columns: dict[str, str] | None = None,
        selected_column: str = "",
        initial_items: list[str] | None = None,
        create_column_callback: Callable[[], bool] | None = None,
    ):
        if custom_columns is None:
            custom_columns = {}
        if initial_items is None:
            initial_items = [""]
        super().__init__(parent)
        debug("create_column_callback=", create_column_callback)
        self.create_column_callback = create_column_callback
        self.current_index = 0
        if create_column_callback is not None:
            self.currentTextChanged.connect(self.current_text_changed)
        self.populate_combo(custom_columns, selected_column, initial_items)

    def populate_combo(
        self,
        custom_columns: dict[str, str],
        selected_column: str | None,
        initial_items: dict[str, str] | list[str] | None = None,
        show_lookup_name: bool = True,
    ):
        if initial_items is None:
            initial_items = [""]
        self.clear()
        self.column_names = []
        selected_idx = 0

        for key in sorted(custom_columns.keys()):
            self.column_names.append(key)
            display_name = (
                "%s (%s)" % (key, custom_columns[key])
                if show_lookup_name
                else custom_columns[key]
            )
            self.addItem(display_name)
            if key == selected_column:
                selected_idx = len(self.column_names) - 1

        if isinstance(initial_items, dict):
            for key in sorted(initial_items.keys()):
                self.column_names.append(key)
                display_name = initial_items[key]
                self.addItem(display_name)
                if key == selected_column:
                    selected_idx = len(self.column_names) - 1
        else:
            for display_name in initial_items:
                self.column_names.append(display_name)
                self.addItem(display_name)
                if display_name == selected_column:
                    selected_idx = len(self.column_names) - 1

        debug("create_column_callback=", self.create_column_callback)
        if self.create_column_callback is not None:
            self.addItem(self.CREATE_NEW_COLUMN_ITEM)

        self.setCurrentIndex(selected_idx)

    def get_selected_column(self) -> str:
        return self.column_names[self.currentIndex()]

    def current_text_changed(self, new_text: str):
        debug("new_text='%s'" % new_text)
        debug(
            "new_text == self.CREATE_NEW_COLUMN_ITEM='%s'"
            % (new_text == self.CREATE_NEW_COLUMN_ITEM)
        )
        if (
            new_text == self.CREATE_NEW_COLUMN_ITEM
            and self.create_column_callback is not None
        ):
            debug("calling callback")
            result = self.create_column_callback()
            if not result:
                debug(
                    "column not created, setting back to original value - ",
                    self.current_index,
                )
                self.setCurrentIndex(self.current_index)
        else:
            self.current_index = self.currentIndex()


class ProgressBar(QDialog):
    def __init__(
        self,
        parent: QWidget | None = None,
        max_items: int = 100,
        window_title: str = "Progress Bar",
        label: str = "Label goes here",
        on_top: bool = False,
    ):
        if on_top:
            super().__init__(parent=parent, flags=Qt.WindowType.WindowStaysOnTopHint)
        else:
            super().__init__(parent=parent)
        self.application = Application
        self.setWindowTitle(window_title)
        self.l = QVBoxLayout(self)
        self.setLayout(self.l)

        self.label = QLabel(label)
        self.l.addWidget(self.label)

        self.progressBar = QProgressBar(self)
        self.progressBar.setRange(0, max_items)
        self.progressBar.setValue(0)
        self.l.addWidget(self.progressBar)

    def show_with_maximum(self, maximum_count: int):
        self.set_maximum(maximum_count)
        self.set_value(0)
        self.show()

    def increment(self):
        self.progressBar.setValue(self.progressBar.value() + 1)
        self.refresh()

    def refresh(self):
        self.application.processEvents()

    def set_label(self, value: str):
        self.label.setText(value)
        self.refresh()

    def left_align_label(self):
        self.label.setAlignment(Qt.AlignmentFlag.AlignLeft)

    def set_maximum(self, value: int):
        self.progressBar.setMaximum(value)
        self.refresh()

    def set_value(self, value: int):
        self.progressBar.setValue(value)
        self.refresh()


class TitleWidgetItem(QTableWidgetItem):
    def __init__(self, book: Book):
        super().__init__(book.title)
        self.title_sort = book.title_sort

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, TitleWidgetItem):
            return self.title_sort < other.title_sort
        return NotImplemented


class AuthorsTableWidgetItem(ReadOnlyTableWidgetItem):
    def __init__(self, authors: list[str], author_sort: str | None = None):
        text = " & ".join(authors)
        ReadOnlyTableWidgetItem.__init__(self, text)
        self.setForeground(Qt.GlobalColor.darkGray)
        self.author_sort = author_sort

    def __lt__(self, other: Any):
        if (
            self.author_sort is not None
            and isinstance(other, AuthorsTableWidgetItem)
            and other.author_sort is not None
        ):
            return self.author_sort < other.author_sort
        return NotImplemented


class ReadingStatusGroupBox(QGroupBox):
    def __init__(self, parent: QWidget):
        QGroupBox.__init__(self, parent)

        self.setTitle(_("Reading status"))
        options_layout = QGridLayout()
        self.setLayout(options_layout)

        self.reading_status_checkbox = QCheckBox(_("Change reading status"), self)
        options_layout.addWidget(self.reading_status_checkbox, 0, 0, 1, 2)
        self.reading_status_checkbox.clicked.connect(
            self.reading_status_checkbox_clicked
        )

        self.unread_radiobutton = QRadioButton(_("Unread"), self)
        options_layout.addWidget(self.unread_radiobutton, 1, 0, 1, 1)
        self.unread_radiobutton.setEnabled(False)

        self.reading_radiobutton = QRadioButton(_("Reading"), self)
        options_layout.addWidget(self.reading_radiobutton, 1, 1, 1, 1)
        self.reading_radiobutton.setEnabled(False)

        self.finished_radiobutton = QRadioButton(_("Finished"), self)
        options_layout.addWidget(self.finished_radiobutton, 1, 2, 1, 1)
        self.finished_radiobutton.setEnabled(False)

        self.reset_position_checkbox = QCheckBox(_("Reset reading position"), self)
        options_layout.addWidget(self.reset_position_checkbox, 2, 0, 1, 3)
        self.reset_position_checkbox.setToolTip(
            _(
                "If this option is checked, the current position and last reading date will be reset."
            )
        )

    def reading_status_checkbox_clicked(self, checked: bool):
        self.unread_radiobutton.setEnabled(checked)
        self.reading_radiobutton.setEnabled(checked)
        self.finished_radiobutton.setEnabled(checked)
        self.reset_position_checkbox.setEnabled(checked)

    def readingStatusIsChecked(self):
        return self.reading_status_checkbox.isChecked()

    def readingStatus(self):
        readingStatus = -1
        if self.unread_radiobutton.isChecked():
            readingStatus = 0
        elif self.reading_radiobutton.isChecked():
            readingStatus = 1
        elif self.finished_radiobutton.isChecked():
            readingStatus = 2

        return readingStatus
