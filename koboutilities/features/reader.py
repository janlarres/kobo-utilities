from __future__ import annotations

import time
from configparser import ConfigParser
from functools import partial
from typing import TYPE_CHECKING, cast

from calibre.devices.kobo.driver import KOBO
from calibre.gui2 import info_dialog, question_dialog
from qt.core import (
    QCheckBox,
    QComboBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME
from ..dialogs import ImageTitleLayout, PluginDialog
from ..utils import debug

if TYPE_CHECKING:
    from calibre.gui2 import ui
    from qt.core import QWidget

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources

LINE_SPACINGS = [1.3, 1.35, 1.4, 1.6, 1.775, 1.9, 2, 2.2, 3]
LINE_SPACINGS_020901 = [
    1,
    1.05,
    1.07,
    1.1,
    1.2,
    1.4,
    1.5,
    1.7,
    1.8,
    2,
    2.2,
    2.4,
    2.6,
    2.8,
    3,
]
LINE_SPACINGS_030200 = [
    1,
    1.05,
    1.07,
    1.1,
    1.2,
    1.35,
    1.5,
    1.7,
    1.8,
    2,
    2.2,
    2.4,
    2.6,
    2.8,
    3,
]
KOBO_FONTS = {
    (0, 0, 0): {  # Format is: Display name, setting name
        "Document Default": "default",
        "Amasis": "Amasis",
        "Avenir": "Avenir Next",
        "Caecilia": "Caecilia",
        "Georgia": "Georgia",
        "Gill Sans": "Gill Sans",
        "Kobo Nickel": "Kobo Nickel",
        "Malabar": "Malabar",
        "Rockwell": "Rockwell",
        "Gothic": "A-OTF Gothic MB101 Pr6N",
        "Ryumin": "A-OTF Ryumin Pr6N",
        "OpenDyslexic": "OpenDyslexic",
    },
    (3, 19, 0): {  # Format is: Display name, setting name
        "Document Default": "default",
        "Amasis": "Amasis",
        "Avenir": "Avenir Next",
        "Caecilia": "Caecilia",
        "Georgia": "Georgia",
        "Gill Sans": "Gill Sans",
        "Kobo Nickel": "Kobo Nickel",
        "Malabar": "Malabar",
        "Rockwell": "Rockwell",
        "Kobo Tsukushi Mincho": "KBJ-TsukuMin Pr6N RB",
        "Kobo UD Kakugo": "KBJ-UDKakugo Pr6N M",
        "OpenDyslexic": "OpenDyslexic",
    },
    (4, 13, 12638): {  # Format is: Display name, setting name
        "Document Default": "default",
        "Amasis": "Amasis",
        "Avenir": "Avenir Next",
        "Caecilia": "Caecilia",
        "Georgia": "Georgia",
        "Gill Sans": "Gill Sans",
        "Kobo Nickel": "Kobo Nickel",
        "Malabar": "Malabar",
        "Rockwell": "Rockwell",
        "AR UDJingxihei": "AR UDJingxihei",
        "Kobo Tsukushi Mincho": "KBJ-TsukuMin Pr6N RB",
        "Kobo UD Kakugo": "KBJ-UDKakugo Pr6N M",
        "OpenDyslexic": "OpenDyslexic",
    },
    (4, 34, 20097): {  # Format is: Display name, setting name
        "Document Default": "default",
        "Amasis": "Amasis",
        "Avenir": "Avenir Next",
        "Caecilia": "Caecilia",
        "Georgia": "Georgia",
        "Gill Sans": "Gill Sans",
        "Kobo Nickel": "Kobo Nickel",
        "Malabar": "Malabar",
        "AR UDJingxihei": "AR UDJingxihei",
        "Kobo Tsukushi Mincho": "KBJ-TsukuMin Pr6N RB",
        "Kobo UD Kakugo": "KBJ-UDKakugo Pr6N M",
        "OpenDyslexic": "OpenDyslexic",
        "Rakuten Serif": "Rakuten Serif",
        "Rakuten Sans": "Rakuten Sans",
    },
}

load_translations()


def set_reader_fonts(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    contentIDs = get_contentIDs_for_selected(gui)

    debug("contentIDs", contentIDs)

    if len(contentIDs) == 0:
        return

    dlg = ReaderOptionsDialog(
        gui, device, contentIDs[0] if len(contentIDs) == 1 else None
    )
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    options = cfg.plugin_prefs.ReadingOptions
    if options.updateConfigFile:
        _update_config_reader_settings(device, options)

    updated_fonts, added_fonts, _deleted_fonts, count_books = _set_reader_fonts(
        device, contentIDs, options
    )
    result_message = (
        _("Change summary:")
        + "\n\t"
        + _(
            "Font settings updated={0}\n\tFont settings added={1}\n\tTotal books={2}"
        ).format(updated_fonts, added_fonts, count_books)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Device library updated"),
        result_message,
        show=True,
    )


def remove_reader_fonts(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
) -> None:
    del dispatcher, load_resources
    current_view = gui.current_view()
    if current_view is None or len(current_view.selectionModel().selectedRows()) == 0:
        return

    contentIDs = get_contentIDs_for_selected(gui)

    if len(contentIDs) == 0:
        return

    mb = question_dialog(
        gui,
        _("Remove reader settings"),
        _("Do you want to remove the reader settings for the selected books?"),
        show_copy_button=False,
    )
    if not mb:
        return

    options = cfg.plugin_prefs.ReadingOptions
    _updated_fonts, _added_fonts, deleted_fonts, _count_books = _set_reader_fonts(
        device, contentIDs, options, delete=True
    )
    result_message = (
        _("Change summary:")
        + "\n\t"
        + _("Font settings deleted={0}").format(deleted_fonts)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Device library updated"),
        result_message,
        show=True,
    )


class ReaderOptionsDialog(PluginDialog):
    def __init__(
        self,
        parent: QWidget,
        device: KoboDevice,
        contentID: str | None,
    ):
        super().__init__(
            parent,
            "kobo utilities plugin:reader font settings dialog",
        )
        self.device = device

        fwversion = cast("tuple[int, int, int]", device.driver.fwversion)
        debug("fwversion=", fwversion)
        self.line_spacings = LINE_SPACINGS
        if fwversion >= (3, 2, 0):
            self.line_spacings = LINE_SPACINGS_030200
        elif fwversion >= (2, 9, 1):
            self.line_spacings = LINE_SPACINGS_020901

        self.font_list = self.get_font_list()
        self.initialize_controls(contentID)

        # Set some default values from last time dialog was used.
        options = cfg.plugin_prefs.ReadingOptions
        self.change_settings(options)
        debug("options", options)
        if options.lockMargins:
            self.lock_margins_checkbox.click()
        if options.updateConfigFile:
            self.update_config_file_checkbox.setChecked(True)
        if options.doNotUpdateIfSet:
            self.do_not_update_if_set_checkbox.setChecked(True)
        self.get_book_settings_pushbutton.setEnabled(contentID is not None)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self, contentID: str | None):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Kobo eReader font settings"),
            "SetReaderFonts",
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("Reader font settings"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        options_layout.addWidget(QLabel(_("Font face")), 0, 0, 1, 1)
        self.font_choice = FontChoiceComboBox(self, self.font_list)
        options_layout.addWidget(self.font_choice, 0, 1, 1, 4)
        options_layout.addWidget(QLabel(_("Font size")), 1, 0, 1, 1)
        self.font_size_spin = QSpinBox(self)
        self.font_size_spin.setMinimum(12)
        self.font_size_spin.setMaximum(58)
        self.font_size_spin.setToolTip(
            _("Font size to use when reading. The device default is about 22.")
        )
        options_layout.addWidget(self.font_size_spin, 1, 1, 1, 1)

        options_layout.addWidget(QLabel(_("Line spacing")), 2, 0, 1, 1)
        self.line_spacing_spin = QSpinBox(self)
        self.line_spacing_spin.setMinimum(0)
        self.line_spacing_spin.setMaximum(len(self.line_spacings) - 1)
        options_layout.addWidget(self.line_spacing_spin, 2, 1, 1, 1)
        self.line_spacing_spin.setToolTip(
            _(
                "The line spacing number is how many times the right arrow is pressed on the device."
            )
        )
        self.line_spacing_spin.valueChanged.connect(self.line_spacing_spin_changed)

        self.custom_line_spacing_checkbox = QCheckBox(_("Custom setting"), self)
        options_layout.addWidget(self.custom_line_spacing_checkbox, 2, 2, 1, 1)
        self.custom_line_spacing_checkbox.setToolTip(
            _(
                "If you want to try a line spacing other than the Kobo specified, check this and enter a number."
            )
        )
        self.custom_line_spacing_checkbox.clicked.connect(
            self.custom_line_spacing_checkbox_clicked
        )

        self.custom_line_spacing_edit = QLineEdit(self)
        self.custom_line_spacing_edit.setEnabled(False)
        options_layout.addWidget(self.custom_line_spacing_edit, 2, 3, 1, 2)
        self.custom_line_spacing_edit.setToolTip(
            _(
                "Kobo use from 1.3 to 4.0. Any number can be entered, but whether the device will use it, is another matter."
            )
        )

        options_layout.addWidget(QLabel(_("Left margins")), 3, 0, 1, 1)
        self.left_margins_spin = QSpinBox(self)
        self.left_margins_spin.setMinimum(0)
        self.left_margins_spin.setMaximum(16)
        self.left_margins_spin.setToolTip(
            _(
                "Margins on the device are set in multiples of two, but single steps work."
            )
        )
        options_layout.addWidget(self.left_margins_spin, 3, 1, 1, 1)
        self.left_margins_spin.valueChanged.connect(self.left_margins_spin_changed)

        self.lock_margins_checkbox = QCheckBox(_("Lock margins"), self)
        options_layout.addWidget(self.lock_margins_checkbox, 3, 2, 1, 1)
        self.lock_margins_checkbox.setToolTip(
            _(
                "Lock the left and right margins to the same value. Changing the left margin will also set the right margin."
            )
        )
        self.lock_margins_checkbox.clicked.connect(self.lock_margins_checkbox_clicked)

        options_layout.addWidget(QLabel(_("Right margins")), 3, 3, 1, 1)
        self.right_margins_spin = QSpinBox(self)
        self.right_margins_spin.setMinimum(0)
        self.right_margins_spin.setMaximum(16)
        self.right_margins_spin.setToolTip(
            _(
                "Margins on the device are set in multiples of three, but single steps work."
            )
        )
        options_layout.addWidget(self.right_margins_spin, 3, 4, 1, 1)

        options_layout.addWidget(QLabel(_("Justification")), 5, 0, 1, 1)
        self.justification_choice = JustificationChoiceComboBox(self)
        options_layout.addWidget(self.justification_choice, 5, 1, 1, 1)

        self.update_config_file_checkbox = QCheckBox(_("Update config file"), self)
        options_layout.addWidget(self.update_config_file_checkbox, 5, 2, 1, 1)
        self.update_config_file_checkbox.setToolTip(
            _(
                "Update the 'Kobo eReader.conf' file with the new settings. These will be used when opening new books or books that do not have stored settings."
            )
        )

        self.do_not_update_if_set_checkbox = QCheckBox(_("Do not update if set"), self)
        options_layout.addWidget(self.do_not_update_if_set_checkbox, 5, 3, 1, 2)
        self.do_not_update_if_set_checkbox.setToolTip(
            _("Do not upate the font settings if it is already set for the book.")
        )

        layout.addStretch(1)

        button_layout = QHBoxLayout(self)
        layout.addLayout(button_layout)
        self.get_device_settings_pushbutton = QPushButton(
            _("&Get configuration from device"), self
        )
        button_layout.addWidget(self.get_device_settings_pushbutton)
        self.get_device_settings_pushbutton.setToolTip(
            _("Read the device configuration file to get the current default settings.")
        )
        self.get_device_settings_pushbutton.clicked.connect(self.get_device_settings)

        self.get_book_settings_pushbutton = QPushButton(
            _("&Get settings from device"), self
        )
        button_layout.addWidget(self.get_book_settings_pushbutton)
        self.get_book_settings_pushbutton.setToolTip(
            _("Fetches the current for the selected book from the device.")
        )
        if contentID is not None:
            self.get_book_settings_pushbutton.clicked.connect(
                partial(self.get_book_settings, contentID)
            )

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        button_layout.addWidget(button_box)

    def ok_clicked(self):
        with cfg.plugin_prefs.ReadingOptions as options:
            options.readingFontFamily = self.font_list[
                self.font_choice.currentText().strip()
            ]
            options.readingAlignment = self.justification_choice.currentText().strip()
            options.readingFontSize = self.font_size_spin.value()
            if self.custom_line_spacing_is_checked():
                options.readingLineHeight = float(self.custom_line_spacing_edit.text())
                debug("custom - readingLineHeight=", options.readingLineHeight)
            else:
                options.readingLineHeight = self.line_spacings[
                    self.line_spacing_spin.value()
                ]
                debug("spin - readingLineHeight=", options.readingLineHeight)
            options.readingLeftMargin = self.left_margins_spin.value()
            options.readingRightMargin = self.right_margins_spin.value()
            options.lockMargins = self.lock_margins_checkbox_is_checked()
            options.updateConfigFile = self.update_config_file_checkbox.isChecked()
            options.doNotUpdateIfSet = self.do_not_update_if_set_checkbox.isChecked()

        self.accept()

    def custom_line_spacing_checkbox_clicked(self, checked: bool):
        self.line_spacing_spin.setEnabled(not checked)
        self.custom_line_spacing_edit.setEnabled(checked)
        if not self.custom_line_spacing_is_checked():
            self.line_spacing_spin_changed(None)

    def lock_margins_checkbox_clicked(self, checked: bool):
        self.right_margins_spin.setEnabled(not checked)
        if checked:  # not self.custom_line_spacing_is_checked():
            self.right_margins_spin.setProperty(
                "value", int(str(self.left_margins_spin.value()))
            )

    def line_spacing_spin_changed(self, checked: bool | None):
        del checked
        self.custom_line_spacing_edit.setText(
            str(self.line_spacings[int(str(self.line_spacing_spin.value()))])
        )

    def left_margins_spin_changed(self, checked: bool):
        del checked
        if self.lock_margins_checkbox_is_checked():
            self.right_margins_spin.setProperty(
                "value", int(str(self.left_margins_spin.value()))
            )

    def custom_line_spacing_is_checked(self):
        return self.custom_line_spacing_checkbox.isChecked()

    def lock_margins_checkbox_is_checked(self):
        return self.lock_margins_checkbox.isChecked()

    def get_device_settings(self):
        koboConfig = ConfigParser(allow_no_value=True)
        device = cast("ui.Main", self.parent()).device_manager.connected_device
        assert isinstance(device, KOBO), (
            f"device is of an unexpected type: {type(device)}"
        )
        device_path = device._main_prefix
        debug("device_path=", device_path)
        assert device_path is not None
        normalized_path = device.normalize_path(
            device_path + ".kobo/Kobo/Kobo eReader.conf"
        )
        assert normalized_path is not None
        koboConfig.read(normalized_path)

        device_settings = cfg.ReadingOptionsConfig()
        if koboConfig.has_option("Reading", cfg.KEY_READING_FONT_FAMILY):
            device_settings.readingFontFamily = koboConfig.get(
                "Reading", cfg.KEY_READING_FONT_FAMILY
            )
        if koboConfig.has_option("Reading", cfg.KEY_READING_ALIGNMENT):
            device_settings.readingAlignment = koboConfig.get(
                "Reading", cfg.KEY_READING_ALIGNMENT
            )
        if koboConfig.has_option("Reading", cfg.KEY_READING_FONT_SIZE):
            device_settings.readingFontSize = int(
                koboConfig.get("Reading", cfg.KEY_READING_FONT_SIZE)
            )
        if koboConfig.has_option("Reading", cfg.KEY_READING_LINE_HEIGHT):
            device_settings.readingLineHeight = float(
                koboConfig.get("Reading", cfg.KEY_READING_LINE_HEIGHT)
            )
        if koboConfig.has_option("Reading", cfg.KEY_READING_LEFT_MARGIN):
            device_settings.readingLeftMargin = int(
                koboConfig.get("Reading", cfg.KEY_READING_LEFT_MARGIN)
            )
        if koboConfig.has_option("Reading", cfg.KEY_READING_RIGHT_MARGIN):
            device_settings.readingRightMargin = int(
                koboConfig.get("Reading", cfg.KEY_READING_RIGHT_MARGIN)
            )

        self.change_settings(device_settings)

    def change_settings(self, reader_settings: cfg.ReadingOptionsConfig):
        font_face = reader_settings.readingFontFamily
        debug("font_face=", font_face)
        self.font_choice.select_text(font_face)

        justification = reader_settings.readingAlignment
        self.justification_choice.select_text(justification)

        font_size = reader_settings.readingFontSize
        self.font_size_spin.setProperty("value", font_size)

        line_spacing = reader_settings.readingLineHeight
        debug("line_spacing='%s'" % line_spacing)
        if line_spacing in self.line_spacings:
            line_spacing_index = self.line_spacings.index(line_spacing)
            debug("line_spacing_index=", line_spacing_index)
            self.custom_line_spacing_checkbox.setChecked(True)
        else:
            self.custom_line_spacing_checkbox.setChecked(False)
            debug("line_spacing_index not found")
            line_spacing_index = 0
        self.custom_line_spacing_checkbox.click()
        self.custom_line_spacing_edit.setText(str(line_spacing))
        self.line_spacing_spin.setProperty("value", line_spacing_index)

        left_margins = reader_settings.readingLeftMargin
        self.left_margins_spin.setProperty("value", left_margins)
        right_margins = reader_settings.readingRightMargin
        self.right_margins_spin.setProperty("value", right_margins)

    def get_book_settings(self, contentID: str):
        book_options = fetch_book_fonts(contentID, self.device)

        if book_options is not None:
            self.change_settings(book_options)

    def get_font_list(self):
        font_list = KOBO_FONTS[(0, 0, 0)]
        for fw_version, fw_font_list in sorted(KOBO_FONTS.items()):
            debug("fw_version=", fw_version)
            if fw_version <= self.device.driver.fwversion:
                debug("found version?=", fw_version)
                font_list = fw_font_list
            else:
                break
        debug("font_list=", font_list)

        return font_list


class FontChoiceComboBox(QComboBox):
    def __init__(self, parent: QWidget, font_list: dict[str, str]):
        QComboBox.__init__(self, parent)
        for name, font in sorted(font_list.items()):
            self.addItem(name, font)

    def select_text(self, selected_text: str):
        idx = self.findData(selected_text)
        if idx != -1:
            self.setCurrentIndex(idx)
        else:
            self.setCurrentIndex(0)


class JustificationChoiceComboBox(QComboBox):
    def __init__(self, parent: QWidget):
        QComboBox.__init__(self, parent)
        self.addItems(["Off", "Left", "Justify"])

    def select_text(self, selected_text: str):
        idx = self.findText(selected_text)
        if idx != -1:
            self.setCurrentIndex(idx)
        else:
            self.setCurrentIndex(0)


def _update_config_reader_settings(
    device: KoboDevice, options: cfg.ReadingOptionsConfig
):
    config_section_reading = "Reading"

    koboConfig, config_file_path = get_config_file(device)

    if not koboConfig.has_section(config_section_reading):
        koboConfig.add_section(config_section_reading)

    koboConfig.set(
        config_section_reading,
        cfg.KEY_READING_FONT_FAMILY,
        options.readingFontFamily,
    )
    koboConfig.set(
        config_section_reading, cfg.KEY_READING_ALIGNMENT, options.readingAlignment
    )
    koboConfig.set(
        config_section_reading,
        cfg.KEY_READING_FONT_SIZE,
        "%g" % options.readingFontSize,
    )
    koboConfig.set(
        config_section_reading,
        cfg.KEY_READING_LINE_HEIGHT,
        "%g" % options.readingLineHeight,
    )
    koboConfig.set(
        config_section_reading,
        cfg.KEY_READING_LEFT_MARGIN,
        "%g" % options.readingLeftMargin,
    )
    koboConfig.set(
        config_section_reading,
        cfg.KEY_READING_RIGHT_MARGIN,
        "%g" % options.readingRightMargin,
    )

    with open(config_file_path, "w") as config_file:
        koboConfig.write(config_file)


def _set_reader_fonts(
    device: KoboDevice,
    contentIDs: list[str],
    options: cfg.ReadingOptionsConfig,
    delete: bool = False,
):
    debug("start")
    updated_fonts = 0
    added_fonts = 0
    deleted_fonts = 0
    count_books = 0

    debug("connected to device database")

    test_query = (
        "SELECT 1 "
        "FROM content_settings "
        "WHERE ContentType = ? "
        "AND ContentId = ?"
    )  # fmt: skip
    delete_query = (
        "DELETE "
        "FROM content_settings "
        "WHERE ContentType = ? "
        "AND ContentId = ?"
    )  # fmt: skip

    add_query = None
    add_values = ()
    update_query = None
    update_values = ()
    if not delete:
        font_face = options.readingFontFamily
        justification = options.readingAlignment.lower()
        justification = (
            None if justification == "Off" or justification == "" else justification
        )
        font_size = options.readingFontSize
        line_spacing = options.readingLineHeight
        left_margins = options.readingLeftMargin
        right_margins = options.readingRightMargin

        add_query = (
            "INSERT INTO content_settings ( "
            '"ContentType", '
            '"DateModified", '
            '"ReadingFontFamily", '
            '"ReadingFontSize", '
            '"ReadingAlignment", '
            '"ReadingLineHeight", '
            '"ReadingLeftMargin", '
            '"ReadingRightMargin", '
            '"ContentID" '
            ") "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        add_values = (
            BOOK_CONTENTTYPE,
            time.strftime(device.timestamp_string, time.gmtime()),
            font_face,
            font_size,
            justification,
            line_spacing,
            left_margins,
            right_margins,
        )
        update_query = (
            "UPDATE content_settings "
            'SET "DateModified" = ?, '
            '"ReadingFontFamily" = ?, '
            '"ReadingFontSize" = ?, '
            '"ReadingAlignment" = ?, '
            '"ReadingLineHeight" = ?, '
            '"ReadingLeftMargin" = ?, '
            '"ReadingRightMargin" = ? '
            "WHERE ContentType = ?  "
            "AND ContentId = ?"
        )
        update_values = (
            time.strftime(device.timestamp_string, time.gmtime()),
            font_face,
            font_size,
            justification,
            line_spacing,
            left_margins,
            right_margins,
            BOOK_CONTENTTYPE,
        )

    with utils.device_database_connection(device) as connection:
        cursor = connection.cursor()
        for contentID in contentIDs:
            test_values = (BOOK_CONTENTTYPE, contentID)
            if delete:
                debug(f"Deleting settings for '{contentID}'")
                cursor.execute(delete_query, test_values)
                deleted_fonts += 1
            elif update_query is not None and add_query is not None:
                cursor.execute(test_query, test_values)
                try:
                    result = next(cursor)
                    debug("found existing row:", result)
                    if not options.doNotUpdateIfSet:
                        debug(
                            f"Updating settings for '{contentID}' with values: {update_values}"
                        )
                        cursor.execute(update_query, (*update_values, contentID))
                        updated_fonts += 1
                except StopIteration:
                    debug(
                        f"Adding settings for '{contentID}' with values: {add_values}"
                    )
                    cursor.execute(add_query, (*add_values, contentID))
                    added_fonts += 1
            count_books += 1

    return updated_fonts, added_fonts, deleted_fonts, count_books


def get_config_file(device: KoboDevice) -> tuple[ConfigParser, str]:
    assert device.driver._main_prefix is not None
    config_file_path = device.driver.normalize_path(
        device.driver._main_prefix + ".kobo/Kobo/Kobo eReader.conf"
    )
    assert config_file_path is not None
    koboConfig = ConfigParser(allow_no_value=True)
    koboConfig.optionxform = str  # type: ignore[reportAttributeAccessIssue]
    debug("config_file_path=", config_file_path)
    try:
        koboConfig.read(config_file_path)
    except Exception as e:
        debug("exception=", e)
        raise

    return koboConfig, config_file_path


def get_contentIDs_for_selected(gui: ui.Main) -> list[str]:
    view = gui.current_view()
    if view is None:
        return []
    if utils.is_device_view(gui):
        rows = view.selectionModel().selectedRows()
        books = [view.model().db[view.model().map[r.row()]] for r in rows]
        contentIDs = [book.contentID for book in books]
    else:
        book_ids: list[int] = view.get_selected_ids()
        contentIDs = get_contentIDs_for_books(book_ids, gui)
        debug("contentIDs=", contentIDs)

    return contentIDs


def get_contentIDs_for_books(book_ids: list[int], gui: ui.Main) -> list[str]:
    contentIDs = []
    for book_id in book_ids:
        contentIDs_for_book = utils.get_contentIDs_from_id(book_id, gui)
        debug("contentIDs", contentIDs_for_book)
        contentIDs.extend(contentIDs_for_book)
    return contentIDs


def fetch_book_fonts(
    contentID: str, device: KoboDevice
) -> cfg.ReadingOptionsConfig | None:
    debug("start")
    connection = utils.device_database_connection(device)
    book_options = cfg.ReadingOptionsConfig()

    fetch_query = (
        "SELECT  "
        '"ReadingFontFamily", '
        '"ReadingFontSize", '
        '"ReadingAlignment", '
        '"ReadingLineHeight", '
        '"ReadingLeftMargin", '
        '"ReadingRightMargin"  '
        "FROM content_settings "
        "WHERE ContentType = ? "
        "AND ContentId = ?"
    )
    fetch_values = (BOOK_CONTENTTYPE, contentID)

    cursor = connection.cursor()
    cursor.execute(fetch_query, fetch_values)
    try:
        result = next(cursor)
    except StopIteration:
        return None

    book_options.readingFontFamily = result[0]
    book_options.readingFontSize = result[1]
    book_options.readingAlignment = result[2].title() if result[2] else "Off"
    book_options.readingLineHeight = result[3]
    book_options.readingLeftMargin = result[4]
    book_options.readingRightMargin = result[5]

    return book_options
