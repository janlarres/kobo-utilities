# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2012-2022, David Forrester <davidfor@internode.on.net>"
__docformat__ = "restructuredtext en"

import ast
import copy
import enum
import json
import traceback
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, TypeVar, cast

from calibre.constants import DEBUG as _DEBUG
from calibre.db.legacy import LibraryDatabase
from calibre.gui2 import choose_dir, error_dialog, gprefs, open_url, question_dialog
from calibre.gui2.dialogs.confirm_delete import confirm
from calibre.gui2.keyboard import ShortcutConfig
from calibre.utils.config import JSONConfig
from qt.core import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QIcon,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QSize,
    QSpinBox,
    Qt,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from .dialogs import (
    CheckableTableWidgetItem,
    CustomColumnComboBox,
    ImageTitleLayout,
    PluginDialog,
    ReadOnlyTableWidgetItem,
    ReadOnlyTextIconWidgetItem,
)
from .utils import debug, get_icon, prompt_for_restart

if TYPE_CHECKING:
    from types import TracebackType

    from calibre.devices.kobo.driver import KOBO
    from calibre.gui2 import ui

    from .action import KoboUtilitiesAction

# Support for CreateNewCustomColumn was added in 5.35.0
try:
    from calibre.gui2.preferences.create_custom_column import CreateNewCustomColumn

    debug("CreateNewCustomColumn is supported")
    SUPPORTS_CREATE_CUSTOM_COLUMN = True
except ImportError:
    debug("CreateNewCustomColumn is not supported")
    SUPPORTS_CREATE_CUSTOM_COLUMN = False  # type: ignore[reportConstantRedefinition]

load_translations()


# Redefine the debug here so the jobs can see it.
DEBUG = _DEBUG

PREFS_NAMESPACE = "KoboUtilitiesPlugin"
PREFS_KEY_SETTINGS = "settings"

KEY_READING_FONT_FAMILY = "readingFontFamily"
KEY_READING_ALIGNMENT = "readingAlignment"
KEY_READING_FONT_SIZE = "readingFontSize"
KEY_READING_LINE_HEIGHT = "readingLineHeight"
KEY_READING_LEFT_MARGIN = "readingLeftMargin"
KEY_READING_RIGHT_MARGIN = "readingRightMargin"

TOKEN_ANY_DEVICE = "*Any Device"  # noqa: S105
TOKEN_CLEAR_SUBTITLE = "*Clear*"  # noqa: S105
TOKEN_FILE_TIMESTAMP = "*filetimestamp"  # noqa: S105
OTHER_SORTS = {TOKEN_FILE_TIMESTAMP: _("* File timestamp")}

CUSTOM_COLUMN_DEFAULT_LOOKUP_READING_LOCATION = "#kobo_reading_location"
CUSTOM_COLUMN_DEFAULT_LOOKUP_LAST_READ = "#kobo_last_read"
CUSTOM_COLUMN_DEFAULT_LOOKUP_RATING = "#kobo_rating"
CUSTOM_COLUMN_DEFAULT_LOOKUP_PERCENT_READ = "#kobo_percent_read"
CUSTOM_COLUMN_DEFAULT_LOOKUP_TIME_SPENT_READING = "#kobo_time_spent_reading"
CUSTOM_COLUMN_DEFAULT_LOOKUP_REST_OF_BOOK_ESTIMATE = "#kobo_rest_of_book_estimate"
CUSTOM_COLUMN_DEFAULTS = {
    CUSTOM_COLUMN_DEFAULT_LOOKUP_READING_LOCATION: {
        "column_heading": _("Kobo reading location"),
        "datatype": "text",
        "description": _("Kobo reading location from the device."),
        "columns_list": "avail_text_columns",
        "config_label": _("Current reading location column:"),
        "config_tool_tip": _(
            "Select a custom column to store the current reading location. The column type must be 'text' or 'comments'. Leave this blank if you do not want to store or restore the current reading location."
        ),
    },
    CUSTOM_COLUMN_DEFAULT_LOOKUP_PERCENT_READ: {
        "column_heading": _("Kobo % read"),
        "datatype": "int",
        "description": _("Percentage read for the book"),
        "columns_list": "avail_number_columns",
        "config_label": _("Percent read column:"),
        "config_tool_tip": _(
            "Column used to store the current percent read. The column type must be 'integer' or 'float'. Leave this blank if you do not want to store or restore the percentage read."
        ),
    },
    CUSTOM_COLUMN_DEFAULT_LOOKUP_RATING: {
        "column_heading": _("Kobo rating"),
        "datatype": "rating",
        "description": _("Rating for the book on the Kobo device."),
        "columns_list": "avail_rating_columns",
        "config_label": _("Rating column:"),
        "config_tool_tip": _(
            "Column used to store the rating. The column type must be a 'integer'. Leave this blank if you do not want to store or restore the rating."
        ),
    },
    CUSTOM_COLUMN_DEFAULT_LOOKUP_LAST_READ: {
        "column_heading": _("Kobo last read"),
        "datatype": "datetime",
        "description": _("When the book was last read on the Kobo device."),
        "columns_list": "avail_date_columns",
        "config_label": _("Last read column:"),
        "config_tool_tip": _(
            "Column used to store when the book was last read. The column type must be a 'Date'. Leave this blank if you do not want to store the last read timestamp."
        ),
    },
    CUSTOM_COLUMN_DEFAULT_LOOKUP_TIME_SPENT_READING: {
        "column_heading": _("Kobo time spent reading"),
        "datatype": "int",
        "description": _("The time already spent reading the book, in seconds."),
        "columns_list": "avail_number_columns",
        "config_label": _("Time spent reading column:"),
        "config_tool_tip": _(
            "Column used to store how much time was spent reading the book, in seconds. The column type must be 'integer'. Leave this blank if you do not want to store the time spent reading the book."
        ),
    },
    CUSTOM_COLUMN_DEFAULT_LOOKUP_REST_OF_BOOK_ESTIMATE: {
        "column_heading": _("Kobo rest of book estimate"),
        "datatype": "int",
        "description": _("The estimate of the time left to read the book, in seconds."),
        "columns_list": "avail_number_columns",
        "config_label": _("Rest of book estimate column:"),
        "config_tool_tip": _(
            "Column used to store the estimate of how much time is left in the book, in seconds. The column type must be 'integer'. Leave this blank if you do not want to store the estimate of the time left."
        ),
    },
}


# This is necessary because JSONConfig defines a __setitem__() that checks
# self.no_commit, but since unpickling doesn't call __init__() it won't be set
# and thus raise an exception.
# See second note below https://docs.python.org/3/library/pickle.html#object.__setstate__
class PicklableJSONConfig(JSONConfig):
    def __new__(cls, *args: Any, **kwargs: Any):
        obj = super().__new__(cls, *args, **kwargs)
        # Set it to True because we don't want to write the file when unpickling
        obj.no_commit = True
        return obj


Self = TypeVar("Self", bound="ConfigWrapper")


class ConfigWrapper:
    def __init__(
        self,
        wrapped_dict: dict[str, Any] | None = None,
        json_config: PicklableJSONConfig | None = None,
    ):
        self._wrapped_dict = wrapped_dict if wrapped_dict is not None else {}
        self._json_config = json_config
        if json_config is None and isinstance(wrapped_dict, PicklableJSONConfig):
            self._json_config = wrapped_dict

        for key, val in self.__annotations__.items():
            if self._is_wrapper(val):
                annot_wrapped_dict = self._wrapped_dict.setdefault(key, {})
                self.__dict__[key] = self._new_wrapper(val, annot_wrapped_dict)
            elif val.startswith(ConfigDictWrapper.__name__):
                annot_wrapped_dict = self._wrapped_dict.setdefault(key, {})
                self.__dict__[key] = self._new_dict(val, annot_wrapped_dict)
            elif key in self._wrapped_dict:
                self.__dict__[key] = self._wrapped_dict[key]
            else:
                try:
                    self._wrapped_dict[key] = getattr(self, key)
                except AttributeError as e:
                    raise AttributeError(
                        f"Config option '{key}' does not have a default value set"
                    ) from e

    @staticmethod
    def _is_wrapper(val: Any) -> bool:
        return val in globals() and issubclass(globals()[val], ConfigWrapper)

    def _new_wrapper(self, name: str, val: dict[str, Any]) -> ConfigWrapper:
        return globals()[name](val, self._json_config)

    def _new_dict(
        self, annot_val: str, wrapped_dict: dict[str, Any]
    ) -> ConfigDictWrapper[ConfigWrapper]:
        parsed = ast.parse(annot_val)
        assert isinstance(parsed.body[0], ast.Expr)
        annotation = parsed.body[0].value
        assert isinstance(annotation, ast.Subscript)

        # Necessary for Python 3.8 support
        if isinstance(annotation.slice, ast.Index):  # pyright: ignore[reportDeprecated]
            val_type = annotation.slice.value.id  # pyright: ignore[reportAttributeAccessIssue]
        else:
            assert isinstance(annotation.slice, ast.Name)
            val_type = annotation.slice.id

        dict_wrapper = ConfigDictWrapper(wrapped_dict, self._json_config)
        for dict_key, dict_val in wrapped_dict.items():
            dict_wrapper[dict_key] = self._new_wrapper(val_type, dict_val)
        return dict_wrapper

    def __str__(self) -> str:
        return json.dumps(self._wrapped_dict, indent=4)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, value: Any):
        if not isinstance(value, ConfigWrapper):
            return NotImplemented
        return self._wrapped_dict == value._wrapped_dict

    def __iter__(self):
        return {
            k: v for k, v in self.__dict__.items() if k in self.__annotations__
        }.items().__iter__()

    def __deepcopy__(self: Self, memo: Any) -> Self:
        """
        Deep copy everything except the _json_config reference,
        so that changing the copy doesn't update the original file
        """
        new = self.__class__()
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            setattr(new, k, copy.deepcopy(v, memo))
        return new

    def __enter__(self: Self) -> Self:
        if self._json_config is not None:
            self._json_config.__enter__()
        return self

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        if self._json_config is not None:
            return self._json_config.__exit__(exc, value, tb)
        return False

    # Workaround for https://github.com/microsoft/pyright/issues/7183
    # so that access to non-existent attributes gets flagged as an error
    if not TYPE_CHECKING:

        def __setattr__(self, name: Any, value: Any) -> None:
            super().__setattr__(name, value)
            if name != "__annotations__" and name in self.__annotations__:
                if isinstance(value, (ConfigWrapper, ConfigDictWrapper)):
                    self._wrapped_dict[name] = value._wrapped_dict
                else:
                    self._wrapped_dict[name] = value
                if self._json_config is not None:
                    self._json_config.commit()


W = TypeVar("W", bound="ConfigWrapper")


class ConfigDictWrapper(Dict[str, W]):
    def __init__(
        self,
        wrapped_dict: dict[str, Any] | None = None,
        json_config: PicklableJSONConfig | None = None,
    ) -> None:
        self._wrapped_dict = wrapped_dict if wrapped_dict is not None else {}
        self._json_config = json_config

    def clear(self) -> None:
        super().clear()
        self._wrapped_dict.clear()

    def __setitem__(self, key: str, value: W) -> None:
        self._wrapped_dict[key] = value._wrapped_dict
        if value._json_config is None:
            value._json_config = self._json_config
        super().__setitem__(key, value)
        if self._json_config is not None:
            self._json_config.commit()

    def __delitem__(self, key: str):
        del self._wrapped_dict[key]
        return super().__delitem__(key)

    def __deepcopy__(self, memo: Any) -> ConfigDictWrapper[W]:
        """
        Deep copy everything except the _json_config reference,
        so that changing the copy doesn't update the original file
        """
        new: ConfigDictWrapper[W] = ConfigDictWrapper()
        for k, v in self.items():
            new[k] = copy.deepcopy(v, memo)
        return new


class BookmarkOptionsConfig(ConfigWrapper):
    backgroundJob: bool = False
    clearIfUnread: bool = False
    doNotStoreIfReopened: bool = False
    rating: bool = True
    readingStatus: bool = True
    setDateToNow: bool = True
    storeBookmarks: bool = True
    storeIfMoreRecent: bool = False


class MetadataOptionsConfig(ConfigWrapper):
    author: bool = False
    authourSort: bool = False
    description: bool = False
    descriptionTemplate: str = ""
    descriptionUseTemplate: bool = False
    isbn: bool = False
    language: bool = False
    published_date: bool = False
    publisher: bool = False
    rating: bool = False
    readingStatus: int = -1
    reading_direction: str = "Default"
    resetPosition: bool = False
    series: bool = False
    setRreadingStatus: bool = False
    set_reading_direction: bool = False
    set_sync_date: bool = False
    subtitle: bool = False
    subtitleTemplate: str = ""
    sync_date_library_date: str = "timestamp"
    title: bool = False
    titleSort: bool = False
    update_KoboEpubs: bool = False
    usePlugboard: bool = False


class ReadingOptionsConfig(ConfigWrapper):
    readingFontFamily: str = "Georgia"
    readingAlignment: str = "Off"
    readingFontSize: int = 22
    readingLineHeight: float = 1.3
    readingLeftMargin: int = 3
    readingRightMargin: int = 3
    lockMargins: bool = False
    updateConfigFile: bool = False
    doNotUpdateIfSet: bool = False


class BackupAnnotationsConfig(ConfigWrapper):
    dest_directory: str = ""


class BackupOptionsStoreConfig(ConfigWrapper):
    backupCopiesToKeepSpin: int = 5
    backupDestDirectory: str = ""
    backupEachCOnnection: bool = False
    backupZipDatabase: bool = True
    doDailyBackp: bool = False


class CleanImagesDirConfig(ConfigWrapper):
    delete_extra_covers: bool = False


class CommonOptionsStoreConfig(ConfigWrapper):
    buttonActionDevice: str = ""
    buttonActionLibrary: str = ""
    individualDeviceOptions: bool = False


class CoverUploadConfig(ConfigWrapper):
    blackandwhite: bool = False
    dithered_covers: bool = False
    keep_cover_aspect: bool = False
    kepub_covers: bool = False
    letterbox: bool = False
    letterbox_color: str = "#000000"
    png_covers: bool = False


class FixDuplicatesOptionsStoreConfig(ConfigWrapper):
    keepNewestShelf: bool = True
    purgeShelves: bool = False


class GetShelvesOptionStoreConfig(ConfigWrapper):
    allBooks: bool = True
    replaceShelves: bool = True


class RemoveAnnotationsAction(enum.IntEnum):
    All = 0
    Selected = 1
    NotOnDevice = 2
    Empty = 3
    NotEmpty = 4


class RemoveAnnotationsConfig(ConfigWrapper):
    removeAnnotAction: RemoveAnnotationsAction = RemoveAnnotationsAction.All


class RemoveCoversConfig(ConfigWrapper):
    kepub_covers: bool = False
    remove_fullsize_covers: bool = False


class RelatedBooksType(enum.IntEnum):
    Series = 0
    Authors = 1


class SetRelatedBooksOptionsStoreConfig(ConfigWrapper):
    relatedBooksType: RelatedBooksType = RelatedBooksType.Series


class DeviceConfig(ConfigWrapper):
    active: bool = True
    location_code: str = "unknown"
    name: str = "unknown"
    serial_no: str = "unknown"
    type: str = "unknown"
    uuid: str = "unknown"
    backupOptionsStore: BackupOptionsStoreConfig


class PluginConfig(ConfigWrapper):
    BookmarkOptions: BookmarkOptionsConfig
    Devices: ConfigDictWrapper[DeviceConfig]
    MetadataOptions: MetadataOptionsConfig
    ReadingOptions: ReadingOptionsConfig
    backupAnnotations: BackupAnnotationsConfig
    backupOptionsStore: BackupOptionsStoreConfig
    cleanImagesDir: CleanImagesDirConfig
    commonOptionsStore: CommonOptionsStoreConfig
    coverUpload: CoverUploadConfig
    fixDuplicatesOptionsStore: FixDuplicatesOptionsStoreConfig
    getShelvesOptionStore: GetShelvesOptionStoreConfig
    removeAnnotations: RemoveAnnotationsConfig
    removeCovers: RemoveCoversConfig
    setRelatedBooksOptionsStore: SetRelatedBooksOptionsStoreConfig
    _version: int = 0


class CustomColumnOptionsConfig(ConfigWrapper):
    currentReadingLocationColumn: str = ""
    lastReadColumn: str = ""
    percentReadColumn: str = ""
    ratingColumn: str = ""
    restOfBookEstimateColumn: str = ""
    timeSpentReadingColumn: str = ""


class StoreOptionsStoreConfig(ConfigWrapper):
    doNotStoreIfReopened: bool = False
    promptToStore: bool = True
    storeIfMoreRecent: bool = False
    storeOnConnect: bool = False


class ProfileConfig(ConfigWrapper):
    forDevice: str | None = TOKEN_ANY_DEVICE
    profileName: str = "Default"
    customColumnOptions: CustomColumnOptionsConfig
    storeOptionsStore: StoreOptionsStoreConfig


class ReadingPositionChangesStoreConfig(ConfigWrapper):
    selectBooksInLibrary: bool = False
    updeateGoodreadsProgress: bool = False


class LibraryConfig(ConfigWrapper):
    SchemaVersion: float = 0.1
    profiles: ConfigDictWrapper[ProfileConfig]
    readingPositionChangesStore: ReadingPositionChangesStoreConfig
    shelvesColumn: str | None = None


# This is where all preferences for this plugin will be stored
plugin_prefs = PluginConfig(PicklableJSONConfig("plugins/Kobo Utilities"))


@dataclass
class CustomColumns:
    current_location: str | None
    percent_read: str | None
    rating: str | None
    last_read: str | None
    time_spent_reading: str | None
    rest_of_book_estimate: str | None


@dataclass
class KoboVersionInfo:
    serial_no: str
    fw_version: str
    model_id: str


@dataclass
class KoboDevice:
    driver: KOBO
    is_kobotouch: bool
    profile: ProfileConfig | None
    backup_config: BackupOptionsStoreConfig
    device_type: str
    drive_info: dict[str, dict[str, str]]
    uuid: str
    version_info: KoboVersionInfo
    supports_series: bool
    supports_series_list: bool
    supports_ratings: bool
    epub_location_like_kepub: bool
    name: str
    path: str
    db_path: str
    device_db_path: str
    is_db_copied: bool
    timestamp_string: str


def migrate_gui_settings(plugin_prefs: PluginConfig) -> None:
    debug("Migrating settings from gui.json")

    configs = (
        ("clean images dir settings dialog", plugin_prefs.cleanImagesDir),
        ("cover upload settings dialog", plugin_prefs.coverUpload),
        ("reader font settings dialog", plugin_prefs.ReadingOptions),
        (
            "backup annotation files settings dialog",
            plugin_prefs.backupAnnotations,
        ),
        (
            "remove annotation files settings dialog",
            plugin_prefs.removeAnnotations,
        ),
        ("remove cover settings dialog", plugin_prefs.removeCovers),
    )

    def get_gui_settings(name: str) -> dict[str, Any]:
        return cast(
            "dict[str, Any]",
            gprefs.get(f"kobo utilities plugin:{name}:settings", {}),
        )

    for gui_key, obj in configs:
        gui_settings = get_gui_settings(gui_key)
        for key, val in gui_settings.items():
            if key in obj.__annotations__:
                setattr(obj, key, val)


def do_config_migrations() -> None:
    if plugin_prefs._version == 0:
        with plugin_prefs:
            migrate_gui_settings(plugin_prefs)
            plugin_prefs._version = 1

    if plugin_prefs._version == 1:
        debug("Migrating device config to use serial numbers")
        with plugin_prefs:
            devices = list(plugin_prefs.Devices.values())
            plugin_prefs.Devices.clear()
            for device in devices:
                plugin_prefs.Devices[device.serial_no] = device
            plugin_prefs._version = 2


def do_library_migrations(db: LibraryDatabase) -> None:
    library_config = get_library_config(db)

    if library_config.SchemaVersion == 0.1:
        # Update device references in profiles to use the serial number
        for profile in library_config.profiles.values():
            if (
                profile.forDevice != TOKEN_ANY_DEVICE
                and profile.forDevice not in plugin_prefs.Devices
            ):
                for device in plugin_prefs.Devices.values():
                    if profile.forDevice == device.uuid:
                        profile.forDevice = device.serial_no
                        break

        library_config.SchemaVersion = 1
        set_library_config(db, library_config)


def get_library_config(db: LibraryDatabase) -> LibraryConfig:
    library_config = None

    if library_config is None:
        library_config = LibraryConfig(
            copy.deepcopy(
                db.prefs.get_namespaced(PREFS_NAMESPACE, PREFS_KEY_SETTINGS, {})
            )
        )
    debug("library_config:", library_config)
    return library_config


def get_profile_info(db: LibraryDatabase, profile_name: str):
    library_config = get_library_config(db)
    if profile_name in library_config.profiles:
        return library_config.profiles[profile_name]
    new_profile = ProfileConfig()
    new_profile.profileName = profile_name
    return new_profile


def get_book_profile_for_device(
    source: LibraryDatabase | ConfigDictWrapper[ProfileConfig], serial_no: str
):
    if isinstance(source, LibraryDatabase):
        library_config = get_library_config(source)
        profiles = library_config.profiles
    else:
        profiles = source
    for profile in profiles.values():
        if profile.forDevice == serial_no:
            return profile
    for profile in profiles.values():
        if profile.forDevice == TOKEN_ANY_DEVICE:
            return profile
    return None


def get_device_name(serial_no: str, default_name: str = _("(Unknown device)")) -> str:
    device = get_device_config(serial_no)
    return device.name if device else default_name


def get_device_config(serial_no: str) -> DeviceConfig | None:
    return plugin_prefs.Devices.get(serial_no)


def set_library_config(db: LibraryDatabase, library_config: LibraryConfig):
    debug("library_config:", library_config)
    db.prefs.set_namespaced(
        PREFS_NAMESPACE, PREFS_KEY_SETTINGS, library_config._wrapped_dict
    )


def get_column_names(
    gui: ui.Main, device: KoboDevice | None, profile_name: str | None = None
):
    if profile_name:
        profile = get_profile_info(gui.current_db, profile_name)
        columns_config = profile.customColumnOptions
    elif device is not None and device.profile is not None:
        columns_config = device.profile.customColumnOptions
    else:
        return CustomColumns(None, None, None, None, None, None)

    debug("columns_config:", columns_config)
    kobo_chapteridbookmarked_column = columns_config.currentReadingLocationColumn
    kobo_percentRead_column = columns_config.percentReadColumn
    rating_column = columns_config.ratingColumn
    last_read_column = columns_config.lastReadColumn
    time_spent_reading_column = columns_config.timeSpentReadingColumn
    rest_of_book_estimate_column = columns_config.restOfBookEstimateColumn

    custom_cols = gui.current_db.field_metadata.custom_field_metadata(
        include_composites=False
    )
    kobo_chapteridbookmarked_column = (
        kobo_chapteridbookmarked_column
        if kobo_chapteridbookmarked_column in custom_cols
        else None
    )
    kobo_percentRead_column = (
        kobo_percentRead_column if kobo_percentRead_column in custom_cols else None
    )
    if rating_column != "rating":
        rating_column = rating_column if rating_column in custom_cols else None
    last_read_column = last_read_column if last_read_column in custom_cols else None
    time_spent_reading_column = (
        time_spent_reading_column if time_spent_reading_column in custom_cols else None
    )
    rest_of_book_estimate_column = (
        rest_of_book_estimate_column
        if rest_of_book_estimate_column in custom_cols
        else None
    )

    return CustomColumns(
        kobo_chapteridbookmarked_column,
        kobo_percentRead_column,
        rating_column,
        last_read_column,
        time_spent_reading_column,
        rest_of_book_estimate_column,
    )


def validate_profile(profile_name: str, gui: ui.Main, device: KoboDevice | None):
    columns_config = None
    if profile_name:
        profile = get_profile_info(gui.current_db, profile_name)
        columns_config = profile.customColumnOptions
    elif device is not None and device.profile is not None:
        columns_config = device.profile.customColumnOptions

    if columns_config is None:
        return "{0}\n\n{1}".format(
            _('Profile "{0}" does not exist.').format(profile_name),
            _("Select another profile to proceed."),
        )

    custom_cols = gui.current_db.field_metadata.custom_field_metadata(
        include_composites=False
    )

    def check_column_name(column_name: str | None):
        return (
            None
            if column_name is None or len(column_name.strip()) == 0
            else column_name
        )

    def check_column_exists(column_name: str | None):
        return column_name is not None and column_name in custom_cols

    debug("columns_config:", columns_config)
    kobo_chapteridbookmarked_column = columns_config.currentReadingLocationColumn
    kobo_percentRead_column = columns_config.percentReadColumn
    rating_column = columns_config.ratingColumn
    last_read_column = columns_config.lastReadColumn

    kobo_chapteridbookmarked_column = check_column_name(kobo_chapteridbookmarked_column)
    kobo_percentRead_column = check_column_name(kobo_percentRead_column)
    rating_column = check_column_name(rating_column)
    last_read_column = check_column_name(last_read_column)

    if (
        kobo_chapteridbookmarked_column is None
        and kobo_percentRead_column is None
        and rating_column is None
        and last_read_column is None
    ):
        return "{0} {1}\n\n{2}".format(
            _('Profile "{0}" is invalid.').format(profile_name),
            _("It has no columns to store the reading status."),
            _("Select another profile to proceed."),
        )

    kobo_chapteridbookmarked_column_exists = check_column_exists(
        kobo_chapteridbookmarked_column
    )
    kobo_percentRead_column_exists = check_column_exists(kobo_percentRead_column)
    if rating_column is not None:
        rating_column_exists = rating_column == "rating" or check_column_exists(
            rating_column
        )
    else:
        rating_column_exists = False
    last_read_column_exists = check_column_exists(last_read_column)

    invalid_columns = []
    if (
        kobo_chapteridbookmarked_column is not None
        and not kobo_chapteridbookmarked_column_exists
    ):
        invalid_columns.append(kobo_chapteridbookmarked_column)
    if kobo_percentRead_column is not None and not kobo_percentRead_column_exists:
        invalid_columns.append(kobo_percentRead_column)
    if rating_column is not None and not rating_column_exists:
        invalid_columns.append(rating_column)
    if last_read_column is not None and not last_read_column_exists:
        invalid_columns.append(last_read_column)

    if len(invalid_columns) > 0:
        invalid_columns_string = ", ".join(
            [f'"{invalid_column}"' for invalid_column in invalid_columns]
        )
        invalid_columns_msg = (
            _("The column {0} does not exist.")
            if len(invalid_columns) == 1
            else _("The columns {0} do not exist.")
        )
        return "{0} {1}\n\n{2}".format(
            _('Profile "{0}" is invalid.').format(profile_name),
            invalid_columns_msg.format(invalid_columns_string),
            _("Select another profile to proceed."),
        )

    return None


class ProfilesTab(QWidget):
    def __init__(self, parent_dialog: ConfigWidget, plugin_action: KoboUtilitiesAction):
        self.parent_dialog = parent_dialog
        QWidget.__init__(self)

        self.plugin_action = plugin_action
        self.library_config = get_library_config(self.plugin_action.gui.current_db)
        debug("self.library_config", self.library_config)
        self.profiles = self.library_config.profiles
        self.current_device_profile = (
            self.plugin_action.device.profile
            if self.plugin_action.device is not None
            else None
        )
        self.profile_name = (
            self.current_device_profile.profileName
            if self.current_device_profile
            else None
        )

        layout = QVBoxLayout(self)
        self.setLayout(layout)

        # -------- Lists configuration ---------
        select_profile_layout = QHBoxLayout()
        layout.addLayout(select_profile_layout)
        profiles_label = QLabel(_("Profiles:"), self)
        select_profile_layout.addWidget(profiles_label)
        self.select_profile_combo = ProfileComboBox(
            self, self.profiles, self.profile_name
        )
        self.select_profile_combo.setMinimumSize(150, 20)
        self.select_profile_combo.currentIndexChanged.connect(
            self._select_profile_combo_changed
        )
        select_profile_layout.addWidget(self.select_profile_combo)
        self.add_profile_button = QToolButton(self)
        self.add_profile_button.setToolTip(_("Add profile"))
        self.add_profile_button.setIcon(QIcon(I("plus.png")))
        self.add_profile_button.clicked.connect(self.add_profile)
        select_profile_layout.addWidget(self.add_profile_button)
        self.delete_profile_button = QToolButton(self)
        self.delete_profile_button.setToolTip(_("Delete profile"))
        self.delete_profile_button.setIcon(QIcon(I("minus.png")))
        self.delete_profile_button.clicked.connect(self.delete_profile)
        select_profile_layout.addWidget(self.delete_profile_button)
        self.rename_profile_button = QToolButton(self)
        self.rename_profile_button.setToolTip(_("Rename profile"))
        self.rename_profile_button.setIcon(get_icon("images/pencil.png"))
        self.rename_profile_button.clicked.connect(self.rename_profile)
        select_profile_layout.addWidget(self.rename_profile_button)
        select_profile_layout.insertStretch(-1)

        # Signal the devices table that the profile selection has changed,
        # so that it can update its profile column
        def signal_devices():
            profile_name = self.select_profile_combo.currentText()
            profile_config = self.profiles[profile_name]
            profile_config.forDevice = self.device_combo.get_selected_device()
            self.parent_dialog.devices_tab.devices_table.refresh_device_profiles(
                self.profiles
            )

        device_layout = QHBoxLayout()
        layout.addLayout(device_layout)
        device_label = QLabel(_("&Device this profile is for:"), self)
        device_label.setToolTip(_("Select the device this profile is for."))
        self.device_combo = DeviceColumnComboBox(self)
        self.device_combo.activated.connect(signal_devices)
        device_label.setBuddy(self.device_combo)
        device_layout.addWidget(device_label)
        device_layout.addWidget(self.device_combo)

        custom_column_group = QGroupBox(_("Custom columns"), self)
        layout.addWidget(custom_column_group)
        options_layout = QGridLayout()
        custom_column_group.setLayout(options_layout)

        self.custom_columns = {}
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_READING_LOCATION] = {
            "current_columns": self.get_text_custom_columns
        }
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_PERCENT_READ] = {
            "current_columns": self.get_number_custom_columns
        }
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_RATING] = {
            "current_columns": self.get_rating_custom_columns
        }
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_LAST_READ] = {
            "current_columns": self.get_date_custom_columns
        }
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_TIME_SPENT_READING] = {
            "current_columns": self.get_number_custom_columns
        }
        self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_REST_OF_BOOK_ESTIMATE] = {
            "current_columns": self.get_number_custom_columns
        }

        self.current_Location_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_READING_LOCATION, 1
        )
        self.percent_read_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_PERCENT_READ, 2
        )
        self.rating_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_RATING, 3
        )
        self.last_read_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_LAST_READ, 4
        )
        self.time_spent_reading_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_TIME_SPENT_READING, 5
        )
        self.rest_of_book_estimate_combo = self.create_custom_column_controls(
            options_layout, CUSTOM_COLUMN_DEFAULT_LOOKUP_REST_OF_BOOK_ESTIMATE, 6
        )

        auto_store_group = QGroupBox(_("Store on connect"), self)
        layout.addWidget(auto_store_group)
        options_layout = QGridLayout()
        auto_store_group.setLayout(options_layout)

        self.store_on_connect_checkbox = QCheckBox(
            _("Store current bookmarks/reading position on connect"), self
        )
        self.store_on_connect_checkbox.setToolTip(
            _(
                "When this is checked, the library will be updated with the current reading position for all books on the device."
            )
        )
        self.store_on_connect_checkbox.clicked.connect(
            self.store_on_connect_checkbox_clicked
        )
        options_layout.addWidget(self.store_on_connect_checkbox, 0, 0, 1, 3)

        self.prompt_to_store_checkbox = QCheckBox(
            _("Prompt to store any changes"), self
        )
        self.prompt_to_store_checkbox.setToolTip(
            _(
                "Enable this to be prompted to save the changed reading positions after an automatic store is done."
            )
        )
        options_layout.addWidget(self.prompt_to_store_checkbox, 1, 0, 1, 1)

        self.store_if_more_recent_checkbox = QCheckBox(_("Only if more recent"), self)
        self.store_if_more_recent_checkbox.setToolTip(
            _(
                "Only store the reading position if the last read timestamp on the device is more recent than in the library."
            )
        )
        options_layout.addWidget(self.store_if_more_recent_checkbox, 1, 1, 1, 1)

        self.do_not_store_if_reopened_checkbox = QCheckBox(
            _("Not if finished in library"), self
        )
        self.do_not_store_if_reopened_checkbox.setToolTip(
            _(
                "Do not store the reading position if the library has the book as finished. This is if the percent read is 100%."
            )
        )
        options_layout.addWidget(self.do_not_store_if_reopened_checkbox, 1, 2, 1, 1)

        layout.addWidget(
            QLabel(
                _(
                    "You can use this site to download the latest version of Kobo firmware:"
                )
            )
        )
        fwurl = "https://pgaskin.net/KoboStuff/kobofirmware.html"
        fwsite = QLabel(f'<a href="{fwurl}">{fwurl}</a>')
        fwsite.setTextInteractionFlags(
            Qt.TextInteractionFlag.LinksAccessibleByMouse
            | Qt.TextInteractionFlag.LinksAccessibleByKeyboard
        )
        fwsite.linkActivated.connect(open_url)
        layout.addWidget(fwsite)

        layout.addStretch(1)

    def create_custom_column_controls(
        self, options_layout: QGridLayout, custom_col_name: str, row_number: int = 1
    ):
        current_Location_label = QLabel(
            CUSTOM_COLUMN_DEFAULTS[custom_col_name]["config_label"], self
        )
        current_Location_label.setToolTip(
            CUSTOM_COLUMN_DEFAULTS[custom_col_name]["config_tool_tip"]
        )
        create_column_callback = (
            partial(self.create_custom_column, custom_col_name)
            if self.parent_dialog.supports_create_custom_column
            else None
        )
        avail_columns = self.custom_columns[custom_col_name]["current_columns"]()
        custom_column_combo = CustomColumnComboBox(
            self, avail_columns, create_column_callback=create_column_callback
        )
        current_Location_label.setBuddy(custom_column_combo)
        options_layout.addWidget(current_Location_label, row_number, 0, 1, 1)
        options_layout.addWidget(custom_column_combo, row_number, 1, 1, 1)
        self.custom_columns[custom_col_name]["combo_box"] = custom_column_combo

        return custom_column_combo

    def _select_profile_combo_changed(self):
        self.persist_profile_config()
        self.refresh_current_profile_info()

    def store_on_connect_checkbox_clicked(self, checked: bool):
        self.prompt_to_store_checkbox.setEnabled(checked)
        self.store_if_more_recent_checkbox.setEnabled(checked)
        self.do_not_store_if_reopened_checkbox.setEnabled(checked)

    # Called by Calibre before save_settings
    def validate(self):
        debug("BEGIN Validate")
        valid = True
        debug("END Validate, status = %s" % valid)
        return valid

    def add_profile(self) -> None:
        debug("Start")
        # Display a prompt allowing user to specify a new profile
        new_profile_name, ok = QInputDialog.getText(
            self,
            _("Add new profile"),
            _("Enter a unique display name for this profile:"),
            text="Default",
        )
        if not ok:
            # Operation cancelled
            return
        new_profile_name = str(new_profile_name).strip()
        # Verify it does not clash with any other profiles in the profile
        for profile_name in self.profiles:
            debug("existing profile: ", profile_name)
            if profile_name.lower() == new_profile_name.lower():
                error_dialog(
                    self,
                    _("Add failed"),
                    _("A profile with the same name already exists"),
                    show=True,
                )
                return

        # As we are about to switch profile, persist the current profiles details if any
        self.persist_profile_config()
        self.profile_name = new_profile_name
        new_profile = ProfileConfig()
        new_profile.profileName = new_profile_name
        self.profiles[new_profile_name] = new_profile
        debug("new profile: ", new_profile)
        # Now update the profiles combobox
        self.select_profile_combo.populate_combo(self.profiles, new_profile_name)
        self.refresh_current_profile_info()
        debug("End")

    def rename_profile(self) -> None:
        if not self.profile_name:
            return
        # Display a prompt allowing user to specify a rename profile
        old_profile_name = self.profile_name
        new_profile_name, ok = QInputDialog.getText(
            self,
            _("Rename profile"),
            _("Enter a new display name for this profile:"),
            text=old_profile_name,
        )
        if not ok:
            # Operation cancelled
            return
        new_profile_name = str(new_profile_name).strip()
        if new_profile_name == old_profile_name:
            return
        # Verify it does not clash with any other profiles in the profile
        for profile_name in self.profiles:
            if profile_name == old_profile_name:
                continue
            if profile_name.lower() == new_profile_name.lower():
                error_dialog(
                    self,
                    _("Add failed"),
                    _("A profile with the same name already exists"),
                    show=True,
                    show_copy_button=False,
                )
                return

        # As we are about to rename profile, persist the current profiles details if any
        self.persist_profile_config()
        self.profiles[new_profile_name] = self.profiles[old_profile_name]
        self.profiles[new_profile_name].profileName = new_profile_name
        del self.profiles[old_profile_name]
        self.profile_name = new_profile_name
        # Now update the profiles combobox
        self.select_profile_combo.populate_combo(self.profiles, new_profile_name)
        self.refresh_current_profile_info()

    def delete_profile(self) -> None:
        if not self.profile_name:
            return
        if len(self.profiles) == 1:
            error_dialog(
                self,
                _("Cannot delete"),
                _("You must have at least one profile"),
                show=True,
                show_copy_button=False,
            )
            return
        if not confirm(
            _(f"Do you want to delete the profile named '{self.profile_name}'"),
            "reading_profile_delete_profile",
            self,
        ):
            return
        del self.profiles[self.profile_name]
        # Now update the profiles combobox
        self.select_profile_combo.populate_combo(self.profiles)
        self.refresh_current_profile_info()

    def refresh_current_profile_info(self):
        debug("Start")
        # Get configuration for the selected profile
        self.profile_name = str(self.select_profile_combo.currentText()).strip()
        profile = get_profile_info(self.plugin_action.gui.current_db, self.profile_name)

        serial_no = profile.forDevice

        # Display profile configuration in the controls
        self.current_Location_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_READING_LOCATION][
                "current_columns"
            ](),
            profile.customColumnOptions.currentReadingLocationColumn,
        )
        self.percent_read_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_PERCENT_READ][
                "current_columns"
            ](),
            profile.customColumnOptions.percentReadColumn,
        )
        self.rating_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_RATING][
                "current_columns"
            ](),
            profile.customColumnOptions.ratingColumn,
        )
        self.last_read_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_LAST_READ][
                "current_columns"
            ](),
            profile.customColumnOptions.lastReadColumn,
        )
        self.time_spent_reading_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_TIME_SPENT_READING][
                "current_columns"
            ](),
            profile.customColumnOptions.timeSpentReadingColumn,
        )
        self.rest_of_book_estimate_combo.populate_combo(
            self.custom_columns[CUSTOM_COLUMN_DEFAULT_LOOKUP_REST_OF_BOOK_ESTIMATE][
                "current_columns"
            ](),
            profile.customColumnOptions.restOfBookEstimateColumn,
        )

        store_prefs = profile.storeOptionsStore
        store_on_connect = store_prefs.storeOnConnect
        prompt_to_store = store_prefs.promptToStore
        store_if_more_recent = store_prefs.storeIfMoreRecent
        do_not_store_if_reopened = store_prefs.doNotStoreIfReopened

        self.device_combo.populate_combo(
            self.parent_dialog.get_devices_list(), serial_no
        )
        self.store_on_connect_checkbox.setChecked(store_on_connect)
        self.prompt_to_store_checkbox.setChecked(prompt_to_store)
        self.prompt_to_store_checkbox.setEnabled(store_on_connect)
        self.store_if_more_recent_checkbox.setChecked(store_if_more_recent)
        self.store_if_more_recent_checkbox.setEnabled(store_on_connect)
        self.do_not_store_if_reopened_checkbox.setChecked(do_not_store_if_reopened)
        self.do_not_store_if_reopened_checkbox.setEnabled(store_on_connect)

        debug("end")

    def persist_profile_config(self):
        debug("Start")
        if not self.profile_name:
            return

        profile_config = self.profiles[self.profile_name]
        debug("profile_config:", profile_config)

        profile_config.forDevice = self.device_combo.get_selected_device()

        store_prefs = profile_config.storeOptionsStore
        store_prefs.storeOnConnect = self.store_on_connect_checkbox.isChecked()
        store_prefs.promptToStore = self.prompt_to_store_checkbox.isChecked()
        store_prefs.storeIfMoreRecent = self.store_if_more_recent_checkbox.isChecked()
        store_prefs.doNotStoreIfReopened = (
            self.do_not_store_if_reopened_checkbox.isChecked()
        )
        debug("store_prefs:", store_prefs)

        column_prefs = profile_config.customColumnOptions
        column_prefs.currentReadingLocationColumn = (
            self.current_Location_combo.get_selected_column()
        )
        debug(
            "column_prefs.currentReadingLocationColumn:",
            column_prefs.currentReadingLocationColumn,
        )
        column_prefs.percentReadColumn = self.percent_read_combo.get_selected_column()
        column_prefs.ratingColumn = self.rating_combo.get_selected_column()
        column_prefs.lastReadColumn = self.last_read_combo.get_selected_column()
        column_prefs.timeSpentReadingColumn = (
            self.time_spent_reading_combo.get_selected_column()
        )
        column_prefs.restOfBookEstimateColumn = (
            self.rest_of_book_estimate_combo.get_selected_column()
        )

        debug("end")

    def get_number_custom_columns(self):
        column_types = ["float", "int"]
        return self.get_custom_columns(column_types)

    def get_rating_custom_columns(self):
        column_types = ["rating", "int"]
        custom_columns = self.get_custom_columns(column_types)
        ratings_column_name = self.plugin_action.gui.library_view.model().orig_headers[
            "rating"
        ]
        custom_columns["rating"] = ratings_column_name
        return custom_columns

    def get_text_custom_columns(self):
        column_types = ["text", "comments"]
        return self.get_custom_columns(column_types)

    def get_date_custom_columns(self):
        column_types = ["datetime"]
        return self.get_custom_columns(column_types)

    def get_custom_columns(self, column_types: list[str]) -> dict[str, str]:
        if self.parent_dialog.supports_create_custom_column:
            assert self.parent_dialog.get_create_new_custom_column_instance is not None
            custom_columns = self.parent_dialog.get_create_new_custom_column_instance.current_columns()
        else:
            custom_columns = self.plugin_action.gui.library_view.model().custom_columns
        available_columns: dict[str, str] = {}
        for key, column in custom_columns.items():
            typ = column["datatype"]
            if typ in column_types and not column["is_multiple"]:
                available_columns[key] = column["name"]
        return available_columns

    def create_custom_column(self, lookup_name: str):
        debug("lookup_name:", lookup_name)
        display_params = {
            "description": CUSTOM_COLUMN_DEFAULTS[lookup_name]["description"]
        }
        datatype = CUSTOM_COLUMN_DEFAULTS[lookup_name]["datatype"]
        column_heading = CUSTOM_COLUMN_DEFAULTS[lookup_name]["column_heading"]

        new_lookup_name = lookup_name

        create_new_custom_column_instance = (
            self.parent_dialog.get_create_new_custom_column_instance
        )
        assert create_new_custom_column_instance is not None
        result = create_new_custom_column_instance.create_column(
            new_lookup_name,
            column_heading,
            datatype,
            False,
            display=display_params,
            generate_unused_lookup_name=True,
            freeze_lookup_name=False,
        )
        debug("result:", result)
        if result[0] == CreateNewCustomColumn.Result.COLUMN_ADDED:  # pyright: ignore[reportPossiblyUnboundVariable]
            self.custom_columns[lookup_name]["combo_box"].populate_combo(
                self.custom_columns[lookup_name]["current_columns"](), result[1]
            )
            return True

        return False


class DevicesTab(QWidget):
    def __init__(self, parent_dialog: ConfigWidget, plugin_action: KoboUtilitiesAction):
        self.devices = copy.deepcopy(plugin_prefs.Devices)
        self.current_device_info = None

        self.parent_dialog = parent_dialog
        QWidget.__init__(self)

        self.plugin_action = plugin_action
        self.gui = plugin_action.gui
        self._connected_device = plugin_action.device
        self.library_config = get_library_config(self.gui.current_db)

        self.individual_device_options = (
            plugin_prefs.commonOptionsStore.individualDeviceOptions
        )

        layout = QVBoxLayout()
        self.setLayout(layout)

        # -------- Device configuration ---------
        devices_group_box = QGroupBox(_("Devices:"), self)
        layout.addWidget(devices_group_box)
        devices_group_box_layout = QVBoxLayout()
        devices_group_box.setLayout(devices_group_box_layout)

        self.devices_table = DevicesTableWidget(self)
        # Note: Do not connect the itemSlectionChanged signale here. It gets done after the table is filled the first time.
        # self.devices_table.itemSelectionChanged.connect(self._devices_table_item_selection_changed)
        devices_group_box_layout.addWidget(self.devices_table)

        buttons_layout = QHBoxLayout()
        devices_group_box_layout.addLayout(buttons_layout)

        self.add_device_btn = QPushButton(_("Add connected device"), self)
        self.add_device_btn.setToolTip(
            _(
                "If you do not have a device connected currently, either plug one\n"
                "in now or exit the dialog and connect to folder/iTunes first"
            )
        )
        self.add_device_btn.setIcon(QIcon(I("plus.png")))
        self.add_device_btn.clicked.connect(self._add_device_clicked)
        buttons_layout.addWidget(self.add_device_btn, 1)

        self.rename_device_btn = QToolButton(self)
        self.rename_device_btn.setIcon(get_icon("images/pencil.png"))
        self.rename_device_btn.setToolTip(_("Rename the currently connected device"))
        self.rename_device_btn.clicked.connect(self._rename_device_clicked)
        buttons_layout.addWidget(self.rename_device_btn)

        self.delete_device_btn = QToolButton(self)
        self.delete_device_btn.setIcon(QIcon(I("minus.png")))
        self.delete_device_btn.setToolTip(_("Delete this device from the device list"))
        self.delete_device_btn.clicked.connect(self._delete_device_clicked)
        self.delete_device_btn.setEnabled(False)
        buttons_layout.addWidget(self.delete_device_btn)

        self.device_options_for_each_checkbox = QCheckBox(
            _("Configure options for each device"), self
        )
        self.device_options_for_each_checkbox.setToolTip(
            _("Selected this option to configure backup for each device.")
        )
        self.device_options_for_each_checkbox.clicked.connect(
            self.device_options_for_each_checkbox_clicked
        )
        if self.individual_device_options:
            self.device_options_for_each_checkbox.setChecked(True)
        layout.addWidget(self.device_options_for_each_checkbox)

        options_layout = QGridLayout()
        self.do_daily_backp_checkbox = QCheckBox(
            _("Back up the device database daily"), self
        )
        self.do_daily_backp_checkbox.setToolTip(
            _(
                "If this is selected the plugin will back up the device database the first time it is connected each day."
            )
        )
        self.do_daily_backp_checkbox.clicked.connect(
            self.do_daily_backp_checkbox_clicked
        )
        options_layout.addWidget(self.do_daily_backp_checkbox, 0, 0, 1, 2)

        self.backup_each_connection_checkbox = QCheckBox(
            _("Back up the device database on each connection"), self
        )
        self.backup_each_connection_checkbox.setToolTip(
            _(
                "If this is selected the plugin will back up the device database each time the device is connected."
            )
        )
        self.backup_each_connection_checkbox.clicked.connect(
            self.backup_each_connection_checkbox_clicked
        )
        options_layout.addWidget(self.backup_each_connection_checkbox, 0, 2, 1, 3)

        self.dest_directory_label = QLabel(_("Destination:"), self)
        self.dest_directory_label.setToolTip(
            _("Select the destination to back up the device database to.")
        )
        self.dest_directory_edit = QLineEdit(self)
        self.dest_directory_edit.setMinimumSize(150, 0)
        self.dest_directory_label.setBuddy(self.dest_directory_edit)
        self.dest_pick_button = QPushButton(_("..."), self)
        self.dest_pick_button.setMaximumSize(24, 20)
        self.dest_pick_button.clicked.connect(self._get_dest_directory_name)
        options_layout.addWidget(self.dest_directory_label, 1, 0, 1, 1)
        options_layout.addWidget(self.dest_directory_edit, 1, 1, 1, 1)
        options_layout.addWidget(self.dest_pick_button, 1, 2, 1, 1)

        self.copies_to_keep_checkbox = QCheckBox(_("Copies to keep"), self)
        self.copies_to_keep_checkbox.setToolTip(
            _(
                "Select this to limit the number of backups kept. If not set, the backup files must be manually deleted."
            )
        )
        self.copies_to_keep_spin = QSpinBox(self)
        self.copies_to_keep_spin.setMinimum(2)
        self.copies_to_keep_spin.setToolTip(
            _("The number of backup copies of the database to keep. The minimum is 2.")
        )
        options_layout.addWidget(self.copies_to_keep_checkbox, 1, 3, 1, 1)
        options_layout.addWidget(self.copies_to_keep_spin, 1, 4, 1, 1)
        self.copies_to_keep_checkbox.clicked.connect(
            self.copies_to_keep_checkbox_clicked
        )

        self.zip_database_checkbox = QCheckBox(
            _("Compress database with config files"), self
        )
        self.zip_database_checkbox.setToolTip(
            _(
                "If checked, the database file will be added to the zip file with configuration files."
            )
        )
        options_layout.addWidget(self.zip_database_checkbox, 2, 0, 1, 3)

        layout.addLayout(options_layout)

        self.toggle_backup_options_state(False)

        layout.insertStretch(-1)

    def on_device_connection_changed(self, is_connected: bool):
        if not is_connected:
            self._connected_device = None
            self.update_from_connection_status()

    def on_device_metadata_available(self):
        if self.plugin_action.device is not None:
            self._connected_device = self.plugin_action.device
            self.update_from_connection_status()

    def _devices_table_item_selection_changed(self):
        debug(
            "len(self.devices_table.selectedIndexes())=",
            len(self.devices_table.selectedIndexes()),
        )
        debug(
            "self.devices_table.selectedIndexes()=",
            self.devices_table.selectedIndexes(),
        )
        if len(self.devices_table.selectedIndexes()) > 0:
            self.delete_device_btn.setEnabled(True)
        else:
            self.delete_device_btn.setEnabled(False)

        self.refresh_current_device_options()

    def _devices_table_current_item_changed(
        self, current: QTableWidgetItem | None, previous: QTableWidgetItem | None
    ) -> None:
        del current
        if (
            previous is not None
            and self.individual_device_options
            and self.current_device_info
        ):
            self.persist_backup_config(self.current_device_info.backupOptionsStore)

    def _add_device_clicked(self):
        if self._connected_device is None:
            debug("self._connected_device is None")
            return

        drive_info = self._connected_device.drive_info
        for location_info in drive_info.values():
            if location_info["location_code"] == "main":
                new_device = DeviceConfig()
                new_device.type = self._connected_device.device_type
                new_device.active = True
                new_device.uuid = location_info["device_store_uuid"]
                new_device.name = location_info["device_name"]
                new_device.location_code = location_info["location_code"]
                new_device.serial_no = self._connected_device.version_info.serial_no
                self.devices[new_device.serial_no] = new_device

        self.devices_table.populate_table(self.devices, self._connected_device)
        self.update_from_connection_status(update_table=False)
        # Ensure the devices combo is refreshed for the current list
        self.parent_dialog.profiles_tab.refresh_current_profile_info()

    def _rename_device_clicked(self) -> None:
        device_info = self.devices_table.get_device_info()
        if not device_info:
            error_dialog(
                self,
                _("Rename failed"),
                _("You must select a device first"),
                show=True,
                show_copy_button=False,
            )
            return

        old_name = device_info.name
        new_device_name, ok = QInputDialog.getText(
            self,
            _("Rename device"),
            _("Enter a new display name for this device:"),
            text=old_name,
        )
        if not ok:
            # Operation cancelled
            return
        new_device_name = str(new_device_name).strip()
        if new_device_name == old_name:
            return
        try:
            self.devices_table.set_current_row_device_name(new_device_name)
        except Exception:
            error_dialog(
                self,
                _("Rename failed"),
                _("An error occurred while renaming."),
                det_msg=traceback.format_exc(),
                show=True,
            )

    def _delete_device_clicked(self) -> None:
        device_info = self.devices_table.get_device_info()
        if not device_info:
            error_dialog(
                self,
                _("Delete failed"),
                _("You must select a device first"),
                show=True,
                show_copy_button=False,
            )
            return
        name = device_info.name
        if not question_dialog(
            self,
            _("Are you sure?"),
            "<p>"
            + _(f"You are about to remove the <b>{name}</b> device from this list. ")
            + _("Are you sure you want to continue?"),
        ):
            return
        self.parent_dialog.profiles_tab.persist_profile_config()
        self.devices_table.delete_selected_row()
        del self.devices[device_info.serial_no]
        self.update_from_connection_status(update_table=False)

        # Ensure the devices combo is refreshed for the current list
        self.parent_dialog.profiles_tab.refresh_current_profile_info()

    def update_from_connection_status(
        self, first_time: bool = False, update_table: bool = True
    ):
        if self._connected_device is None or self.plugin_action.device is None:
            self.add_device_btn.setEnabled(False)
        else:
            # Check to see whether we are connected to a device we already know about
            is_new_device = True
            drive_info = self._connected_device.drive_info
            if drive_info:
                # This is a non iTunes device that we can check to see if we have the serial number for
                serial_no = self._connected_device.version_info.serial_no
                if serial_no in self.devices:
                    is_new_device = False
            else:
                # This is a device without drive info like iTunes
                device_type = self._connected_device.device_type
                if device_type in self.devices:
                    is_new_device = False
            self.add_device_btn.setEnabled(is_new_device)
        if update_table:
            self.devices_table.populate_table(self.devices, self._connected_device)
            self.refresh_current_device_options()
        if first_time:
            self.devices_table.itemSelectionChanged.connect(
                self._devices_table_item_selection_changed
            )
            self.devices_table.currentItemChanged.connect(
                self._devices_table_current_item_changed
            )
            self._devices_table_item_selection_changed()

    def toggle_backup_options_state(self, enabled: bool):
        self.dest_directory_edit.setEnabled(enabled)
        self.dest_pick_button.setEnabled(enabled)
        self.dest_directory_label.setEnabled(enabled)
        self.copies_to_keep_checkbox.setEnabled(enabled)
        self.copies_to_keep_checkbox_clicked(
            enabled and self.copies_to_keep_checkbox.isChecked()
        )
        self.zip_database_checkbox.setEnabled(enabled)

    def do_daily_backp_checkbox_clicked(self, checked: bool):
        enable_backup_options = (
            checked or self.backup_each_connection_checkbox.isChecked()
        )
        self.toggle_backup_options_state(enable_backup_options)
        if self.backup_each_connection_checkbox.isChecked():
            self.backup_each_connection_checkbox.setChecked(False)

    def backup_each_connection_checkbox_clicked(self, checked: bool):
        enable_backup_options = checked or self.do_daily_backp_checkbox.isChecked()
        self.toggle_backup_options_state(enable_backup_options)
        if self.do_daily_backp_checkbox.isChecked():
            self.do_daily_backp_checkbox.setChecked(False)

    def device_options_for_each_checkbox_clicked(self, checked: bool):
        self.individual_device_options = (
            checked or self.device_options_for_each_checkbox.isChecked()
        )
        self.refresh_current_device_options()

    def copies_to_keep_checkbox_clicked(self, checked: bool):
        self.copies_to_keep_spin.setEnabled(checked)

    def _get_dest_directory_name(self):
        path = choose_dir(
            self,
            "back up annotations destination dialog",
            _("Choose backup destination"),
        )
        if path:
            self.dest_directory_edit.setText(path)

    def refresh_current_device_options(self):
        backup_prefs = plugin_prefs.backupOptionsStore
        if self.individual_device_options:
            self.current_device_info = self.devices_table.get_device_info()
            if self.current_device_info is not None:
                backup_prefs = self.current_device_info.backupOptionsStore
            else:
                backup_prefs = BackupOptionsStoreConfig()

        do_daily_backup = backup_prefs.doDailyBackp
        backup_each_connection = backup_prefs.backupEachCOnnection
        copies_to_keep = backup_prefs.backupCopiesToKeepSpin

        self.do_daily_backp_checkbox.setChecked(do_daily_backup)
        self.backup_each_connection_checkbox.setChecked(backup_each_connection)
        self.dest_directory_edit.setText(backup_prefs.backupDestDirectory)
        self.zip_database_checkbox.setChecked(backup_prefs.backupZipDatabase)
        if copies_to_keep == -1:
            self.copies_to_keep_checkbox.setChecked(False)
        else:
            self.copies_to_keep_checkbox.setChecked(True)
            self.copies_to_keep_spin.setProperty("value", copies_to_keep)
        if do_daily_backup:
            self.do_daily_backp_checkbox_clicked(do_daily_backup)
        if backup_each_connection:
            self.backup_each_connection_checkbox_clicked(backup_each_connection)

    def persist_backup_config(self, backup_prefs: BackupOptionsStoreConfig) -> None:
        backup_prefs.doDailyBackp = self.do_daily_backp_checkbox.isChecked()
        backup_prefs.backupEachCOnnection = (
            self.backup_each_connection_checkbox.isChecked()
        )
        backup_prefs.backupZipDatabase = self.zip_database_checkbox.isChecked()
        backup_prefs.backupDestDirectory = self.dest_directory_edit.text()
        backup_prefs.backupCopiesToKeepSpin = (
            self.copies_to_keep_spin.value()
            if self.copies_to_keep_checkbox.isChecked()
            else -1
        )
        debug("backup_prefs:", backup_prefs)

    def persist_devices_config(self):
        debug("Start")

        with plugin_prefs:
            if self.individual_device_options and self.current_device_info:
                self.persist_backup_config(self.current_device_info.backupOptionsStore)
            else:
                self.persist_backup_config(plugin_prefs.backupOptionsStore)

            plugin_prefs.Devices = self.devices
            plugin_prefs.commonOptionsStore.individualDeviceOptions = (
                self.individual_device_options
            )

        debug("end")


class DeviceColumnComboBox(QComboBox):
    def __init__(self, parent: ProfilesTab):
        QComboBox.__init__(self, parent)
        self.device_ids = [None, TOKEN_ANY_DEVICE]

    def populate_combo(
        self, devices: ConfigDictWrapper[DeviceConfig], selected_serial_no: str | None
    ):
        self.clear()
        self.addItem("")
        self.addItem(TOKEN_ANY_DEVICE)
        selected_idx = 0
        if selected_serial_no == TOKEN_ANY_DEVICE:
            selected_idx = 1
        for idx, key in enumerate(devices.keys()):
            self.addItem("%s" % (devices[key].name))
            self.device_ids.append(key)
            if key == selected_serial_no:
                selected_idx = idx + 2
        self.setCurrentIndex(selected_idx)

    def get_selected_device(self):
        return self.device_ids[self.currentIndex()]


class DevicesTableWidget(QTableWidget):
    def __init__(self, parent: DevicesTab):
        QTableWidget.__init__(self, parent)
        self.parent_dialog = parent
        self.plugin_action: KoboUtilitiesAction = parent.plugin_action
        self.serial_no_to_row = {}
        self.setSortingEnabled(False)
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.setMinimumSize(380, 0)

    def populate_table(
        self,
        devices: ConfigDictWrapper[DeviceConfig],
        connected_device: KoboDevice | None,
    ):
        self.clear()
        self.setRowCount(len(devices))
        header_labels = [
            _("Menu"),
            _("Name"),
            _("Model"),
            _("Profile"),
            _("Serial number"),
            _("FW version"),
            _("Status"),
        ]
        self.setColumnCount(len(header_labels))
        self.setHorizontalHeaderLabels(header_labels)
        vert_header = self.verticalHeader()
        assert vert_header is not None
        vert_header.setDefaultSectionSize(32)
        horiz_header = self.horizontalHeader()
        assert horiz_header is not None
        horiz_header.setStretchLastSection(False)
        self.setIconSize(QSize(32, 32))

        for row, serial_no in enumerate(devices.keys()):
            self.serial_no_to_row[serial_no] = row
            self.populate_table_row(row, devices[serial_no], connected_device)

        self.cellChanged.connect(self.update_device_name)
        self.resizeColumnsToContents()
        self.setMinimumColumnWidth(1, 100)
        self.selectRow(0)

    def setMinimumColumnWidth(self, col: int, minimum: int):
        if self.columnWidth(col) < minimum:
            self.setColumnWidth(col, minimum)

    def populate_table_row(
        self,
        row: int,
        device_config: DeviceConfig,
        connected_device: KoboDevice | None,
    ):
        debug("device_config:", device_config)
        device_type = device_config.type
        serial_no = device_config.serial_no
        device_icon = "reader.png"
        is_connected = False
        if connected_device is not None and self.plugin_action.device is not None:
            debug("connected_device:", connected_device)
            is_connected = (
                device_type == connected_device.device_type
                and serial_no == connected_device.version_info.serial_no
            )
        device = self.plugin_action.device
        version_info = device.version_info if device is not None else None
        fw_version = version_info.fw_version if is_connected and version_info else ""
        connected_icon = "images/device_connected.png" if is_connected else None
        debug("connected_icon=%s" % connected_icon)

        name_widget = QTableWidgetItem(device_config.name)
        name_widget.setIcon(get_icon(device_icon))
        name_widget.setFlags(
            Qt.ItemFlag.ItemIsSelectable
            | Qt.ItemFlag.ItemIsEnabled
            | Qt.ItemFlag.ItemIsEditable
        )
        name_widget.setData(Qt.ItemDataRole.UserRole, (device_config, is_connected))

        type_widget = ReadOnlyTableWidgetItem(device_config.type)
        serial_no = device_config.serial_no
        serial_no_widget = ReadOnlyTableWidgetItem(serial_no)
        version_no_widget = ReadOnlyTableWidgetItem(fw_version)

        profile = get_book_profile_for_device(
            self.plugin_action.gui.current_db, serial_no
        )
        profile_widget = ReadOnlyTableWidgetItem(
            profile.profileName if profile is not None else "—"
        )

        self.setItem(row, 0, CheckableTableWidgetItem(device_config.active))
        self.setItem(row, 1, name_widget)
        self.setItem(row, 2, type_widget)
        self.setItem(row, 3, profile_widget)
        self.setItem(row, 4, serial_no_widget)
        self.setItem(row, 5, version_no_widget)
        self.setItem(row, 6, ReadOnlyTextIconWidgetItem("", get_icon(connected_icon)))

    def refresh_device_profiles(
        self, profiles: ConfigDictWrapper[ProfileConfig]
    ) -> None:
        for row in range(self.rowCount()):
            serial_no = cast("ReadOnlyTableWidgetItem", self.item(row, 4)).text()
            profile = get_book_profile_for_device(profiles, serial_no)
            profile_widget = cast("ReadOnlyTableWidgetItem", self.item(row, 3))
            profile_widget.setText(profile.profileName if profile is not None else "—")

    def get_device_info(self, row: int | None = None) -> DeviceConfig | None:
        if row is None and self.currentRow() >= 0:
            row = self.currentRow()
        if row is not None:
            widget = self.item(row, 4)
            if widget is None:
                return None
            serial_no = widget.text()
            return self.parent_dialog.devices[serial_no]
        return None

    def update_device_name(
        self, row: int, column: int, new_name: str | None = None
    ) -> None:
        # We only care about the name column
        if column != 1:
            return
        widget = self.item(row, column)
        assert widget is not None
        if new_name is None:
            new_name = widget.text()
        device_config = self.get_device_info(row)
        if device_config is None or device_config.name == new_name:
            return
        device_config.name = new_name
        widget.setText(new_name)
        self.parent_dialog.parent_dialog.profiles_tab.refresh_current_profile_info()

    def set_current_row_device_name(self, device_name: str):
        if self.currentRow() >= 0:
            self.update_device_name(self.currentRow(), 1, device_name)

    def delete_selected_row(self):
        if self.currentRow() >= 0:
            self.removeRow(self.currentRow())


class OtherTab(QWidget):
    def __init__(self, parent_dialog: ConfigWidget):
        self.parent_dialog = parent_dialog
        QWidget.__init__(self)
        layout = QVBoxLayout()
        self.setLayout(layout)

        other_options_group = QGroupBox(_("Other options"), self)
        layout.addWidget(other_options_group)
        options_layout = QGridLayout()
        other_options_group.setLayout(options_layout)

        library_default_label = QLabel(_("&Library button default:"), self)
        library_default_label.setToolTip(
            _(
                "If plugin is placed as a toolbar button, choose a default action when clicked on"
            )
        )
        self.library_default_combo = SimpleComboBox(
            self,
            self.parent_dialog.plugin_action.library_actions_map,
            plugin_prefs.commonOptionsStore.buttonActionLibrary,
        )
        library_default_label.setBuddy(self.library_default_combo)
        options_layout.addWidget(library_default_label, 0, 0, 1, 1)
        options_layout.addWidget(self.library_default_combo, 0, 1, 1, 2)

        device_default_label = QLabel(_("&Device button default:"), self)
        device_default_label.setToolTip(
            _(
                "If plugin is placed as a toolbar button, choose a default action when clicked on"
            )
        )
        self.device_default_combo = SimpleComboBox(
            self,
            self.parent_dialog.plugin_action.device_actions_map,
            plugin_prefs.commonOptionsStore.buttonActionDevice,
        )
        device_default_label.setBuddy(self.device_default_combo)
        options_layout.addWidget(device_default_label, 1, 0, 1, 1)
        options_layout.addWidget(self.device_default_combo, 1, 1, 1, 2)

        keyboard_shortcuts_button = QPushButton(_("Keyboard shortcuts..."), self)
        keyboard_shortcuts_button.setToolTip(
            _("Edit the keyboard shortcuts associated with this plugin")
        )
        keyboard_shortcuts_button.clicked.connect(parent_dialog.edit_shortcuts)
        layout.addWidget(keyboard_shortcuts_button)

        view_prefs_button = QPushButton(_("&View library preferences..."), self)
        view_prefs_button.setToolTip(
            _("View data stored in the library database for this plugin")
        )
        view_prefs_button.clicked.connect(parent_dialog.view_prefs)
        layout.addWidget(view_prefs_button)

        layout.insertStretch(-1)

    def persist_other_config(self):
        plugin_prefs.commonOptionsStore.buttonActionDevice = (
            self.device_default_combo.currentText()
        )
        plugin_prefs.commonOptionsStore.buttonActionLibrary = (
            self.library_default_combo.currentText()
        )


class ConfigWidget(QWidget):
    def __init__(self, plugin_action: KoboUtilitiesAction):
        debug("Initializing...")
        QWidget.__init__(self)
        self.plugin_action = plugin_action
        layout = QVBoxLayout(self)
        self.setLayout(layout)

        self._get_create_new_custom_column_instance = None
        self.supports_create_custom_column = SUPPORTS_CREATE_CUSTOM_COLUMN

        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Kobo Utilities options"),
            "ConfigurationDialog",
        )
        layout.addLayout(title_layout)

        tab_widget = QTabWidget(self)
        layout.addWidget(tab_widget)

        self.profiles_tab = ProfilesTab(self, plugin_action)
        self.devices_tab = DevicesTab(self, plugin_action)
        self.other_tab = OtherTab(self)
        tab_widget.addTab(self.profiles_tab, _("Profiles"))
        tab_widget.addTab(self.devices_tab, _("Devices"))
        tab_widget.addTab(self.other_tab, _("Other"))

        # Force an initial display of list information
        self.devices_tab.update_from_connection_status(first_time=True)
        self.profiles_tab.refresh_current_profile_info()

        self.connect_signals()

    def connect_signals(self):
        self.plugin_action.plugin_device_connection_changed.connect(
            self.devices_tab.on_device_connection_changed
        )
        self.plugin_action.plugin_device_metadata_available.connect(
            self.devices_tab.on_device_metadata_available
        )

    def get_devices_list(self):
        return self.devices_tab.devices

    def save_settings(self):
        device_prefs = self.get_devices_list()
        plugin_prefs.Devices = device_prefs

        # We only need to update the store for the current list, as switching lists
        # will have updated the other lists
        self.profiles_tab.persist_profile_config()
        self.other_tab.persist_other_config()
        self.devices_tab.persist_devices_config()

        library_config = self.profiles_tab.library_config
        set_library_config(self.plugin_action.gui.current_db, library_config)

    def edit_shortcuts(self):
        self.save_settings()
        # Force the menus to be rebuilt immediately, so we have all our actions registered
        self.plugin_action.rebuild_menus()
        d = KeyboardConfigDialog(
            self.plugin_action.gui, self.plugin_action.action_spec[0]
        )
        if d.exec() == d.DialogCode.Accepted:
            self.plugin_action.gui.keyboard.finalize()

    def view_prefs(self):
        d = PrefsViewerDialog(self.plugin_action.gui, PREFS_NAMESPACE)
        d.exec()

    @property
    def get_create_new_custom_column_instance(self) -> CreateNewCustomColumn | None:
        if (
            self._get_create_new_custom_column_instance is None
            and self.supports_create_custom_column
        ):
            self._get_create_new_custom_column_instance = CreateNewCustomColumn(  # pyright: ignore[reportPossiblyUnboundVariable]
                self.plugin_action.gui
            )
        return self._get_create_new_custom_column_instance


class PrefsViewerDialog(PluginDialog):
    def __init__(self, gui: ui.Main, namespace: str):
        super().__init__(gui, _("Prefs viewer dialog"))
        self.setWindowTitle(_("Preferences for: {}").format(namespace))

        self.gui = gui
        self.db = gui.current_db
        self.namespace = namespace
        self._init_controls()
        self.resize_dialog()

        self._populate_settings()

        if self.keys_list.count():
            self.keys_list.setCurrentRow(0)

    def _init_controls(self):
        layout = QVBoxLayout(self)
        self.setLayout(layout)

        ml = QHBoxLayout()
        layout.addLayout(ml, 1)

        self.keys_list = QListWidget(self)
        self.keys_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.keys_list.setFixedWidth(150)
        self.keys_list.setAlternatingRowColors(True)
        ml.addWidget(self.keys_list)
        self.value_text = QTextEdit(self)
        self.value_text.setReadOnly(False)
        ml.addWidget(self.value_text, 1)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._apply_changes)
        button_box.rejected.connect(self.reject)
        self.clear_button = button_box.addButton(
            _("Clear"), QDialogButtonBox.ButtonRole.ResetRole
        )
        assert self.clear_button is not None
        self.clear_button.setIcon(get_icon("trash.png"))
        self.clear_button.setToolTip(_("Clear all settings for this plugin"))
        self.clear_button.clicked.connect(self._clear_settings)
        layout.addWidget(button_box)

    def _populate_settings(self):
        self.keys_list.clear()
        ns_prefix = self._get_ns_prefix()
        keys = sorted(
            [
                k[len(ns_prefix) :]
                for k in list(self.db.prefs.keys())
                if k.startswith(ns_prefix)
            ]
        )
        for key in keys:
            self.keys_list.addItem(key)
        self.keys_list.setMinimumWidth(self.keys_list.sizeHintForColumn(0))
        self.keys_list.currentRowChanged[int].connect(self._current_row_changed)

    def _current_row_changed(self, new_row: int):
        if new_row < 0:
            self.value_text.clear()
            return
        current_item = self.keys_list.currentItem()
        assert current_item is not None
        key = str(current_item.text())
        val = self.db.prefs.get_namespaced(self.namespace, key, "")
        self.value_text.setPlainText(self.db.prefs.to_raw(val))

    def _get_ns_prefix(self):
        return "namespaced:%s:" % self.namespace

    def _apply_changes(self):
        from calibre.gui2.dialogs.confirm_delete import confirm

        message = (
            "<p>Are you sure you want to change your settings in this library for this plugin?</p>"
            "<p>Any settings in other libraries or stored in a JSON file in your calibre plugins "
            "folder will not be touched.</p>"
            "<p>You must restart calibre afterwards.</p>"
        )
        if not confirm(message, self.namespace + "_clear_settings", self):
            return

        val = self.db.prefs.raw_to_object(str(self.value_text.toPlainText()))
        current_item = self.keys_list.currentItem()
        assert current_item is not None
        key = str(current_item.text())
        self.db.prefs.set_namespaced(self.namespace, key, val)

        restart = prompt_for_restart(
            self,
            "Settings changed",
            "<p>Settings for this plugin in this library have been changed.</p>"
            "<p>Please restart calibre now.</p>",
        )
        self.close()
        if restart:
            self.gui.quit(restart=True)

    def _clear_settings(self):
        from calibre.gui2.dialogs.confirm_delete import confirm

        message = (
            "<p>Are you sure you want to clear your settings in this library for this plugin?</p>"
            "<p>Any settings in other libraries or stored in a JSON file in your calibre plugins "
            "folder will not be touched.</p>"
            "<p>You must restart calibre afterwards.</p>"
        )
        if not confirm(message, self.namespace + "_clear_settings", self):
            return

        ns_prefix = self._get_ns_prefix()
        keys = [k for k in list(self.db.prefs.keys()) if k.startswith(ns_prefix)]
        for k in keys:
            del self.db.prefs[k]
        self._populate_settings()
        restart = prompt_for_restart(
            self,
            _("Settings deleted"),
            _("<p>All settings for this plugin in this library have been cleared.</p>")
            + _("<p>Please restart calibre now.</p>"),
        )
        self.close()
        if restart:
            self.gui.quit(restart=True)


class KeyboardConfigDialog(PluginDialog):
    """
    This dialog is used to allow editing of keyboard shortcuts.
    """

    def __init__(self, gui: ui.Main, group_name: str):
        super().__init__(gui, "Keyboard shortcut dialog")
        self.gui = gui
        self.setWindowTitle("Keyboard shortcuts")
        layout = QVBoxLayout(self)
        self.setLayout(layout)

        self.keyboard_widget = ShortcutConfig(self)
        layout.addWidget(self.keyboard_widget)
        self.group_name = group_name

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.commit)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()
        self.initialize()

    def initialize(self):
        self.keyboard_widget.initialize(self.gui.keyboard)
        self.keyboard_widget.highlight_group(self.group_name)

    def commit(self):
        self.keyboard_widget.commit()
        self.accept()


class ProfileComboBox(QComboBox):
    def __init__(
        self,
        parent: QWidget,
        profiles: ConfigDictWrapper[ProfileConfig],
        selected_text: str | None = None,
    ):
        super().__init__(parent)
        self.populate_combo(profiles, selected_text)

    def populate_combo(
        self,
        profiles: ConfigDictWrapper[ProfileConfig],
        selected_text: str | None = None,
    ):
        self.blockSignals(True)
        self.clear()
        for list_name in sorted(profiles.keys()):
            self.addItem(list_name)
        self.select_view(selected_text)
        self.blockSignals(False)

    def select_view(self, selected_text: str | None):
        self.blockSignals(True)
        if selected_text:
            idx = self.findText(selected_text)
            self.setCurrentIndex(idx)
        elif self.count() > 0:
            self.setCurrentIndex(0)
        self.blockSignals(False)


class SimpleComboBox(QComboBox):
    def __init__(self, parent: QWidget, values: list[str], selected_value: str):
        super().__init__(parent)
        self.values = values
        self.populate_combo(selected_value)

    def populate_combo(self, selected_value: str):
        self.clear()
        selected_idx = idx = -1
        for value in sorted(self.values):
            idx = idx + 1
            self.addItem(value)
            if value == selected_value:
                selected_idx = idx
        self.setCurrentIndex(selected_idx)

    def selected_key(self):
        for value in list(self.values):
            if value == str(self.currentText()).strip():
                return value
        return None
