# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2012-2017, David Forrester <davidfor@internode.on.net>"
__docformat__ = "restructuredtext en"

import calendar
import os
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, cast

from calibre.constants import numeric_version as calibre_version
from calibre.devices.kobo.driver import KOBO, KOBOTOUCH
from calibre.gui2 import info_dialog
from calibre.gui2.actions import InterfaceAction, menu_action_unique_name
from calibre.gui2.device import device_signals
from calibre.gui2.dialogs.message_box import MessageBox
from qt.core import QMenu, Qt, QTimer, pyqtSignal

from . import ActionKoboUtilities
from . import config as cfg
from .config import KoboDevice, KoboVersionInfo
from .constants import GUI_NAME
from .features import (
    analytics,
    annotations,  # pyright: ignore[reportDuplicateImport]
    backup,
    booksnotindb,
    cleanimages,
    covers,
    database,
    duplicateshelves,
    getshelves,
    locations,
    manageseries,
    metadata,
    reader,
    readingstatus,
    relatedbooks,
    removeannotations,
    toc,
)
from .utils import debug, get_icon, is_device_view, set_plugin_icon_resources, show_help

if TYPE_CHECKING:
    from calibre.db.legacy import LibraryDatabase
    from calibre.gui2 import ui
    from qt.core import QAction

    from .utils import Dispatcher, LoadResources

PLUGIN_ICONS = [
    "images/icon.png",
    "images/logo_kobo.png",
    "images/manage_series.png",
    "images/lock.png",
    "images/lock32.png",
    "images/lock_delete.png",
    "images/lock_open.png",
    "images/pencil.png",
    "images/sort.png",
    "images/ms_ff.png",
    "images/device_connected.png",
    "images/clock.png",
    "images/clock-history.png",
    "images/database.png",
    "images/databases.png",
    "images/vise.png",
]


KOBO_ROOT_DIR_NAME = ".kobo"
KOBO_EPOCH_CONF_NAME = "epoch.conf"

load_translations()


class KoboUtilitiesAction(InterfaceAction):
    interface_action_base_plugin: ActionKoboUtilities
    qaction: QAction

    name = "KoboUtilities"
    # Create our top-level menu/toolbar action (text, icon_path, tooltip, keyboard shortcut)
    action_spec = (name, None, ActionKoboUtilities.description, ())
    action_type = "current"

    timestamp_string = None

    plugin_device_connection_changed = pyqtSignal(object)
    plugin_device_metadata_available = pyqtSignal()

    def genesis(self):
        # The attribute in the super class gets assigned dynamically
        base = self.interface_action_base_plugin
        self.version = base.name + " v%d.%d.%d" % base.version
        self.gui: ui.Main = self.gui

        self.menu = QMenu(self.gui)
        icon_resources = self.load_resources(PLUGIN_ICONS)
        set_plugin_icon_resources(self.name, icon_resources)
        self.device_actions_map = []
        self.library_actions_map = []
        self.no_device_actions_map = []
        self.menu_actions = {}

        # Assign our menu to this action and an icon
        self.qaction.setMenu(self.menu)
        self.qaction.setIcon(get_icon(PLUGIN_ICONS[0]))
        self.qaction.triggered.connect(self.toolbar_button_clicked)
        self.menu.aboutToShow.connect(self.about_to_show_menu)
        self.menus_lock = threading.RLock()

        self.device: KoboDevice | None = None

        debug(f"Running in {'normal' if __debug__ else 'optimized'} mode")

    def initialization_complete(self):
        cfg.do_config_migrations()
        cfg.do_library_migrations(self.gui.current_db)

        # otherwise configured hot keys won't work until the menu's
        # been displayed once.
        self.rebuild_menus()
        # Subscribe to device connection events
        device_signals.device_connection_changed.connect(
            self._on_device_connection_changed
        )
        device_signals.device_metadata_available.connect(
            self._on_device_metadata_available
        )

    def about_to_show_menu(self):
        self.rebuild_menus()

    def library_changed(self, db: LibraryDatabase):
        cfg.do_library_migrations(db)

        # We need to reset our menus after switching libraries
        self.rebuild_menus()
        device = self.device
        if (
            device is not None
            and device.profile
            and device.profile.storeOptionsStore.storeOnConnect
        ):
            debug("About to do auto store")
            QTimer.singleShot(
                1000,
                lambda: locations.auto_store_current_bookmark(
                    device,
                    self.gui,
                    cast("Dispatcher", self.Dispatcher),
                    cast("LoadResources", self.load_resources),
                ),
            )

    def set_toolbar_button_tooltip(self):
        text = ActionKoboUtilities.description
        text += "\n"
        if self.device is not None:
            debug(
                "device connected. self.device.fwversion=",
                self.device.driver.fwversion,
            )
            text += "\n"
            text += _("Connected device: ")
            text += self.device.name
            text += "\n"
            text += _("Firmware version: ")
            text += ".".join([str(i) for i in self.device.driver.fwversion])
        text += "\n"
        text += _("Driver: ")
        text += self.device_driver_name

        debug("setting to text='%s'" % text)
        a = self.qaction
        a.setToolTip(text)

    def _on_device_connection_changed(self, is_connected: bool):
        debug(f"Connection changed; is_connected: {is_connected}")
        self.plugin_device_connection_changed.emit(is_connected)
        if not is_connected:
            self.device = None
            self.rebuild_menus()
        else:
            self.device = get_device(self.gui)

        self.set_toolbar_button_tooltip()

    def _on_device_metadata_available(self):
        debug("Start")
        self.device = get_device(self.gui)
        self.plugin_device_metadata_available.emit()
        self.set_toolbar_button_tooltip()

        if self.device is not None:
            profile = self.device.profile
            backup_config = self.device.backup_config
            debug("profile:", profile)
            debug("backup_config:", backup_config)
            if backup_config.doDailyBackp or backup_config.backupEachCOnnection:
                debug("About to start auto backup")
                backup.auto_backup_device_database(
                    self.device, self.gui, cast("Dispatcher", self.Dispatcher)
                )

            if profile and profile.storeOptionsStore.storeOnConnect:
                debug("About to start auto store")
                locations.auto_store_current_bookmark(
                    self.device,
                    self.gui,
                    cast("Dispatcher", self.Dispatcher),
                    self.load_resources,
                )

        self.rebuild_menus()

    def rebuild_menus(self) -> None:
        def menu_wrapper(
            func: Callable[
                [KoboDevice, ui.Main, Dispatcher, LoadResources],
                None,
            ],
        ):
            def wrapper():
                if self.device is None:
                    raise AssertionError(_("No device connected."))
                func(
                    self.device,
                    self.gui,
                    cast("Dispatcher", self.Dispatcher),
                    self.load_resources,
                )

            return wrapper

        with self.menus_lock:
            # Show the config dialog
            # The config dialog can also be shown from within
            # Preferences->Plugins, which is why the do_user_config
            # method is defined on the base plugin class
            self.menu.clear()
            for action in self.menu_actions.values():
                self.gui.keyboard.unregister_shortcut(
                    action.calibre_shortcut_unique_name
                )
                # starting in calibre 2.10.0, actions are registers at
                # the top gui level for OSX' benefit.
                self.gui.removeAction(action)
            self.menu_actions = {}
            self.device_actions_map = []
            self.library_actions_map = []
            self.no_device_actions_map = []

            device = self.device
            self.menu.setToolTipsVisible(True)
            self.set_toolbar_button_tooltip()

            self.create_menu_item_ex(
                self.menu,
                _("&Set reader font for selected books"),
                unique_name="Set reader font for selected books",
                shortcut_name=_("Set reader font for selected books"),
                image="embed-fonts.png",
                triggered=menu_wrapper(reader.set_reader_fonts),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Remove reader font for selected books"),
                unique_name="Remove reader font for selected books",
                shortcut_name=_("Remove reader font for selected books"),
                triggered=menu_wrapper(reader.remove_reader_fonts),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )

            self.menu.addSeparator()

            self.create_menu_item_ex(
                self.menu,
                _("Update &metadata in device library"),
                unique_name="Update metadata in device library",
                shortcut_name=_("Update metadata in device library"),
                image="metadata.png",
                triggered=menu_wrapper(metadata.update_metadata),
                is_library_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Change reading status in device library"),
                unique_name="Change reading status in device library",
                shortcut_name=_("Change reading status in device library"),
                triggered=menu_wrapper(readingstatus.change_reading_status),
                is_device_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Manage series information in device library"),
                unique_name="Manage series information in device library",
                shortcut_name=_("Manage series information in device library"),
                triggered=menu_wrapper(manageseries.manage_series_on_device),
                is_device_action=True,
                is_supported=device is not None and device.supports_series,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Store/restore reading positions"),
                unique_name="Store/restore reading positions",
                shortcut_name=_("Store/restore reading positions"),
                image="bookmarks.png",
                triggered=menu_wrapper(locations.handle_bookmarks),
                is_library_action=True,
            )

            self.menu.addSeparator()
            self.create_menu_item_ex(
                self.menu,
                _("&Update ToC for selected books"),
                image="toc.png",
                unique_name="Update ToC for selected books",
                shortcut_name=_("Update ToC for selected books"),
                triggered=menu_wrapper(toc.update_book_toc_on_device),
                is_library_action=True,
            )

            self.menu.addSeparator()
            self.create_menu_item_ex(
                self.menu,
                _("&Upload covers for selected books"),
                unique_name="Upload covers for selected books",
                shortcut_name=_("Upload covers for selected books"),
                image="default_cover.png",
                triggered=menu_wrapper(covers.upload_covers),
                is_library_action=True,
            )
            self.create_menu_item_ex(
                self.menu,
                _("&Remove covers for selected books"),
                unique_name="Remove covers for selected books",
                shortcut_name=_("Remove covers for selected books"),
                triggered=menu_wrapper(covers.remove_covers),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Clean images directory of extra cover images"),
                unique_name="Clean images directory of extra cover images",
                shortcut_name=_("Clean images directory of extra cover images"),
                triggered=menu_wrapper(cleanimages.clean_images_dir),
                is_library_action=True,
                is_device_action=True,
            )
            self.create_menu_item_ex(
                self.menu,
                _("&Open cover image directory"),
                unique_name="Open cover image directory",
                shortcut_name=_("Open cover image directory"),
                triggered=menu_wrapper(covers.open_cover_image_directory),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )
            self.menu.addSeparator()

            self.create_menu_item_ex(
                self.menu,
                _("Get collections from device"),
                unique_name="Get collections from device",
                shortcut_name=_("Get collections from device"),
                image="catalog.png",
                triggered=menu_wrapper(getshelves.get_shelves_from_device),
                is_library_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )
            if device is not None and device.driver.fwversion < (4, 4, 0):
                self.create_menu_item_ex(
                    self.menu,
                    _("Set related books"),
                    unique_name="Set related books",
                    shortcut_name=_("Set related books"),
                    triggered=menu_wrapper(relatedbooks.set_related_books),
                    is_library_action=True,
                    is_device_action=True,
                    is_supported=device.supports_series,
                )
            self.menu.addSeparator()
            self.create_menu_item_ex(
                self.menu,
                _("Copy annotations for selected books"),
                image="edit_input.png",
                unique_name="Copy annotations for selected books",
                shortcut_name=_("Copy annotations for selected books"),
                triggered=menu_wrapper(annotations.getAnnotationForSelected),
                is_library_action=True,
            )
            self.create_menu_item_ex(
                self.menu,
                _("Back up EPUB annotation files"),
                unique_name="Back up EPUB annotation files",
                shortcut_name=_("Back up EPUB annotation files"),
                triggered=menu_wrapper(annotations.backup_annotation_files),
                is_library_action=True,
            )
            self.create_menu_item_ex(
                self.menu,
                _("Remove EPUB annotation files"),
                unique_name="Remove EPUB annotation files",
                shortcut_name=_("Remove EPUB annotation files"),
                triggered=menu_wrapper(removeannotations.remove_annotations_files),
                is_library_action=True,
                is_device_action=True,
            )

            self.menu.addSeparator()

            self.create_menu_item_ex(
                self.menu,
                _("Show books not in the device database"),
                unique_name="Show books not in the device database",
                shortcut_name=_("Show books not in the device database"),
                triggered=menu_wrapper(booksnotindb.show_books_not_in_database),
                is_device_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("Refresh the list of books on the device"),
                unique_name="Refresh the list of books on the device",
                shortcut_name=_("Refresh the list of books on the device"),
                image="view-refresh.png",
                triggered=lambda _: self.gui.device_detected(True, KOBOTOUCH),
                is_library_action=True,
                is_device_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("Restore backup to device"),
                unique_name="Restore backup to device",
                shortcut_name=_("Restore backup to device"),
                image="images/clock-history.png",
                triggered=menu_wrapper(backup.restore_backup),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None,
            )

            databaseMenu = cast("QMenu", self.menu.addMenu(_("Database")))
            databaseMenu.setIcon(get_icon("images/database.png"))
            self.create_menu_item_ex(
                databaseMenu,
                _("Block analytics events"),
                unique_name="Block analytics events",
                shortcut_name=_("Block analytics events"),
                triggered=menu_wrapper(analytics.block_analytics),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )
            databaseMenu.addSeparator()
            self.create_menu_item_ex(
                databaseMenu,
                _("Fix duplicate collections"),
                unique_name="Fix duplicate collections",
                shortcut_name=_("Fix duplicate collections"),
                triggered=menu_wrapper(duplicateshelves.fix_duplicate_shelves),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and device.is_kobotouch,
            )
            self.create_menu_item_ex(
                databaseMenu,
                _("Check the device database"),
                unique_name="Check the device database",
                shortcut_name=_("Check the device database"),
                image="ok.png",
                triggered=menu_wrapper(database.check_device_database),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and not device.is_db_copied,
                not_supported_reason=_("Not supported for this connection mode"),
            )
            self.create_menu_item_ex(
                databaseMenu,
                _("Compress the device database"),
                unique_name="Compress the device database",
                shortcut_name=_("Compress the device database"),
                image="images/vise.png",
                triggered=menu_wrapper(database.vacuum_device_database),
                is_library_action=True,
                is_device_action=True,
                is_supported=device is not None and not device.is_db_copied,
                not_supported_reason=_("Not supported for this connection mode"),
            )
            self.create_menu_item_ex(
                databaseMenu,
                _("Back up device database"),
                unique_name="Back up device database",
                shortcut_name=_("Back up device database"),
                image="images/databases.png",
                triggered=menu_wrapper(backup.backup_device_database),
                is_library_action=True,
                is_device_action=True,
            )

            self.menu.addSeparator()
            self.create_menu_item_ex(
                self.menu,
                _("Set time on device"),
                unique_name="Set time on device",
                shortcut_name=_("Set time on device"),
                image="images/clock.png",
                tooltip=_(
                    "Creates a file on the device which will be used to set the time when the device is disconnected."
                ),
                triggered=menu_wrapper(set_time_on_device),
                is_library_action=True,
                is_device_action=True,
            )

            self.menu.addSeparator()

            def create_configure_driver_item(menu: QMenu, menu_text: str):
                self.create_menu_item_ex(
                    menu,
                    menu_text,
                    unique_name="Configure driver",
                    shortcut_name=_("Configure driver"),
                    image="config.png",
                    triggered=self.configure_device,
                    is_library_action=True,
                    is_device_action=True,
                    is_no_device_action=True,
                )

            # Calibre 8 integrates the functionality of the KoboTouchExtended driver
            # and disables the plugin, so there is no need to switch between drivers.
            # Cast the version literal because of https://github.com/microsoft/pyright/issues/7733
            if calibre_version >= cast("tuple[int, int, int]", (8, 0, 0)):
                create_configure_driver_item(self.menu, _("&Configure driver..."))
            else:
                driver_menu = self.menu.addMenu(_("Driver"))
                assert driver_menu is not None
                create_configure_driver_item(
                    driver_menu,
                    _("&Configure current driver") + " - " + self.device_driver_name,
                )
                self.create_menu_item_ex(
                    driver_menu,
                    _("Switch between main and extended driver"),
                    unique_name="Switch between main and extended driver",
                    shortcut_name=_("Switch between main and extended driver"),
                    image="config.png",
                    triggered=self.switch_device_driver,
                    is_library_action=True,
                    is_device_action=True,
                    is_no_device_action=True,
                )
            self.menu.addSeparator()

            self.create_menu_item_ex(
                self.menu,
                _("&Customize plugin") + "...",  # shortcut=False,
                unique_name="Customize plugin",
                shortcut_name=_("Customize plugin"),
                image="config.png",
                triggered=self.show_configuration,
                is_library_action=True,
                is_device_action=True,
                is_no_device_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&Help"),  # shortcut=False,
                unique_name="Help",
                shortcut_name=_("Help"),
                image="help.png",
                triggered=lambda _: show_help(),
                is_library_action=True,
                is_device_action=True,
                is_no_device_action=True,
            )

            self.create_menu_item_ex(
                self.menu,
                _("&About plugin"),  # shortcut=False,
                image="images/icon.png",
                unique_name="About KoboUtilities",
                shortcut_name=_("About KoboUtilities"),
                triggered=self.about,
                is_library_action=True,
                is_device_action=True,
                is_no_device_action=True,
            )

            self.gui.keyboard.finalize()

    def about(self):
        # Get the about text from a file inside the plugin zip file
        # The get_resources function is a builtin function defined for all your
        # plugin code. It loads files from the plugin zip file. It returns
        # the bytes from the specified file.
        #
        # Note that if you are loading more than one file, for performance, you
        # should pass a list of names to get_resources. In this case,
        # get_resources will return a dictionary mapping names to bytes. Names that
        # are not found in the zip file will not be in the returned dictionary.

        about_text = "{0}\n\n{1}".format(
            self.version, get_resources("about.md").decode("utf-8")
        )
        debug("self.version=", self.version)
        debug("about_text=", about_text)
        changelog = get_resources("CHANGELOG.md")
        changelog = changelog.decode("utf-8")
        dialog = MessageBox(
            MessageBox.INFO,
            GUI_NAME,
            about_text,
            changelog,
            q_icon=self.qaction.icon(),
            show_copy_button=False,
            parent=self.gui,
        )
        dialog.msg.setOpenExternalLinks(True)
        dialog.msg.setWordWrap(True)
        dialog.msg.setTextFormat(Qt.TextFormat.MarkdownText)
        dialog.det_msg.setMarkdown(changelog)
        dialog.do_resize()
        dialog.exec()

    def create_menu_item_ex(
        self,
        parent_menu: QMenu,
        menu_text: str,
        triggered: Callable[[], None] | Callable[[QAction], None],
        image: str | None = None,
        shortcut: str | list[str] | None | Literal[False] = None,
        is_checked: bool | None = None,
        shortcut_name: str | None = None,
        unique_name: str | None = None,
        tooltip: str | None = None,
        is_library_action: bool = False,
        is_device_action: bool = False,
        is_no_device_action: bool = False,
        is_supported: bool = True,
        not_supported_reason: str = _("Not supported for this device"),
    ) -> QAction:
        if self.device is None and not is_no_device_action:
            tooltip = _("No device connected")
            enabled = False
        elif self.device is not None and not is_supported:
            tooltip = not_supported_reason
            enabled = False
        elif is_device_view(self.gui) and not is_device_action:
            tooltip = _("Only supported in library view")
            enabled = False
        elif not is_device_view(self.gui) and not is_library_action:
            tooltip = _("Only supported in device view")
            enabled = False
        else:
            tooltip = tooltip
            enabled = True

        orig_shortcut = shortcut
        kb = self.gui.keyboard
        if unique_name is None:
            unique_name = menu_text
        if shortcut is not False:
            full_unique_name = menu_action_unique_name(self, unique_name)
            if full_unique_name in kb.shortcuts:
                shortcut = False
            else:
                if shortcut is not None and isinstance(shortcut, str):
                    shortcut = None if len(shortcut) == 0 else _(shortcut)

        if shortcut_name is None:
            shortcut_name = menu_text.replace("&", "")

        ac = self.create_menu_action(
            parent_menu,
            unique_name,
            menu_text,
            icon=None,
            shortcut=shortcut,
            description=tooltip,
            triggered=triggered,
            shortcut_name=shortcut_name,
        )
        if (
            shortcut is False
            and orig_shortcut is not False
            and ac.calibre_shortcut_unique_name in self.gui.keyboard.shortcuts
        ):
            kb.replace_action(ac.calibre_shortcut_unique_name, ac)
        if image:
            ac.setIcon(get_icon(image))
        if is_checked is not None:
            ac.setCheckable(True)
            if is_checked:
                ac.setChecked(True)
        ac.setEnabled(enabled)
        self.menu_actions[shortcut_name] = ac

        if is_library_action:
            self.library_actions_map.append(shortcut_name)
        if is_device_action:
            self.device_actions_map.append(shortcut_name)
        if is_no_device_action:
            self.no_device_actions_map.append(shortcut_name)

        return ac

    def toolbar_button_clicked(self):
        self.rebuild_menus()

        if is_device_view(self.gui):
            assert self.device is not None
            if self.device.supports_series:
                button_action = cfg.plugin_prefs.commonOptionsStore.buttonActionDevice
                if button_action == "":
                    self.show_configuration()
                else:
                    self.menu_actions[button_action].trigger()
            else:
                readingstatus.change_reading_status(
                    self.device,
                    self.gui,
                    cast("Dispatcher", self.Dispatcher),
                    self.load_resources,
                )
        else:
            button_action = cfg.plugin_prefs.commonOptionsStore.buttonActionLibrary
            if button_action == "":
                debug("no button action")
                self.show_configuration()
            else:
                try:
                    debug("self.no_device_actions_map=", self.no_device_actions_map)
                    if self.device or button_action in self.no_device_actions_map:
                        self.menu_actions[button_action].trigger()
                    else:
                        self.show_configuration()
                except Exception as e:
                    debug(
                        "exception running button action:",
                        button_action,
                        " exception: ",
                        e,
                    )
                    self.show_configuration()

    @property
    def device_driver_name(self) -> str:
        if self.device:
            device_driver_name = self.device.driver.name
        else:
            from calibre.customize.ui import is_disabled

            try:
                from calibre_plugins.kobotouch_extended.device.driver import (  # type: ignore[reportMissingImports]
                    KOBOTOUCHEXTENDED,
                )

                cuurent_driver = (
                    KOBOTOUCHEXTENDED
                    if not is_disabled(KOBOTOUCHEXTENDED)
                    else KOBOTOUCH
                )
            except Exception as e:
                debug("could not load extended driver. Exception=", e)
                cuurent_driver = KOBOTOUCH
            device_driver_name = cuurent_driver.name

        return device_driver_name

    def configure_device(self):
        if self.device:
            self.gui.configure_connected_device()
        else:
            from calibre.customize.ui import is_disabled

            try:
                from calibre_plugins.kobotouch_extended.device.driver import (  # type: ignore[reportMissingImports]
                    KOBOTOUCHEXTENDED,
                )

                driver_to_configure = (
                    KOBOTOUCHEXTENDED
                    if not is_disabled(KOBOTOUCHEXTENDED)
                    else KOBOTOUCH
                )
            except Exception as e:
                debug("could not load extended driver. Exception=", e)
                driver_to_configure = KOBOTOUCH
            driver_to_configure = driver_to_configure(None)
            driver_to_configure.do_user_config(self.gui)

    def switch_device_driver(self):
        from calibre.customize.ui import disable_plugin, enable_plugin, is_disabled

        try:
            from calibre_plugins.kobotouch_extended.device.driver import (  # type: ignore[reportMissingImports]
                KOBOTOUCHEXTENDED,
            )
        except Exception as e:
            debug("could not load extended driver. Exception=", e)
            result_message = _(
                "The KoboTouchExtended driver is not installed. There is nothing to switch between, so no changes have been made."
            )
            info_dialog(
                self.gui,
                _("Kobo Utilities") + " - " + _("Switch device drivers"),
                result_message,
                show=True,
            )
            return

        extended_disabled = is_disabled(KOBOTOUCHEXTENDED)
        main_disabled = is_disabled(KOBOTOUCH)
        debug(
            "using is_disabled: main_disabled=%s, extended_disabled=%s"
            % (main_disabled, extended_disabled)
        )
        if extended_disabled:
            enable_plugin(KOBOTOUCHEXTENDED)
            disable_plugin(KOBOTOUCH)
            result_message = _(
                "The KoboTouch driver has been disabled and the KoboTouchExtended driver has been enabled."
            )
        else:
            enable_plugin(KOBOTOUCH)
            disable_plugin(KOBOTOUCHEXTENDED)
            result_message = _(
                "The KoboTouchExtended driver has been disabled and the KoboTouch driver has been enabled."
            )
        result_message += "\n" + _(
            "You will need to restart calibre for this change to be applied."
        )
        self.check_if_restart_needed(
            restart_message=result_message, restart_needed=True
        )

        self.set_toolbar_button_tooltip()
        return

    def show_configuration(self):
        debug("before do_user_config")
        restart_message = _(
            "Calibre must be restarted before the plugin can be configured."
        )
        # Check if a restart is needed. If the restart is needed, but the user does not
        # trigger it, the result is true and we do not do the configuration.
        if self.check_if_restart_needed(restart_message=restart_message):
            return

        self.interface_action_base_plugin.do_user_config(self.gui)
        debug("after do_user_config")
        restart_message = _(
            "New custom columns have been created."
            "\nYou will need to restart calibre for this change to be applied."
        )
        self.check_if_restart_needed(restart_message=restart_message)

    def check_if_restart_needed(
        self, restart_message: str | None = None, restart_needed: bool = False
    ):
        if self.gui.must_restart_before_config or restart_needed:
            if restart_message is None:
                restart_message = _(
                    "Calibre must be restarted before the plugin can be configured."
                )
            from calibre.gui2 import show_restart_warning

            do_restart = show_restart_warning(restart_message)
            if do_restart:
                debug("restarting calibre...")
                self.gui.quit(restart=True)
            else:
                debug("calibre needs to be restarted, do not open configuration")
                return True
        return False


def set_time_on_device(
    device: KoboDevice,
    gui: ui.Main,
    dispatcher: Dispatcher,
    load_resources: LoadResources,
):
    del dispatcher, load_resources
    debug("start")
    now = calendar.timegm(time.gmtime())
    debug("time=%s" % now)
    epoch_conf_path = os.path.join(
        device.path, KOBO_ROOT_DIR_NAME, KOBO_EPOCH_CONF_NAME
    )
    with open(epoch_conf_path, "w") as epoch_conf:
        epoch_conf.write("%s" % now)
    gui.status_bar.show_message(
        _("Kobo Utilities") + " - " + _("Time file created on device."), 3000
    )
    debug("end")


def get_device(gui: ui.Main):
    try:
        device = gui.device_manager.connected_device
        debug(f"Connected device: {device}")
        if device is None or not isinstance(device, KOBO):
            debug("No supported Kobo device appears to be connected")
            return None
    except Exception:
        debug("No device connected")
        return None

    try:
        # This method got added in Calibre 5.41
        device_version_info = device.device_version_info()
    except AttributeError:
        debug(
            "no KOBO.device_version_info() method found; assuming old Calibre version"
        )
        version_file = Path(str(device._main_prefix), ".kobo/version")
        device_version_info = version_file.read_text().strip().split(",")
        debug("manually read version:", device_version_info)

    serial_no, _, fw_version, _, _, model_id = device_version_info
    version_info = KoboVersionInfo(serial_no, fw_version, model_id)

    device_path = device._main_prefix
    debug('device_path="%s"' % device_path)
    current_device_information = gui.device_manager.get_current_device_information()
    if not device_path or not current_device_information:
        # No device actually connected or it isn't ready
        return None
    connected_device_info = current_device_information.get("info", None)
    assert connected_device_info is not None
    debug("device_info:", connected_device_info)
    device_type = connected_device_info[0]
    drive_info = cast("dict[str, dict[str, str]]", connected_device_info[4])
    library_db = gui.library_view.model().db
    device_uuid = drive_info["main"]["device_store_uuid"]
    current_device_profile = cfg.get_book_profile_for_device(library_db, serial_no)
    current_device_config = cfg.get_device_config(serial_no)
    device_name = cfg.get_device_name(serial_no, device.gui_name)
    debug("device_name:", device_name)
    individual_device_options = (
        cfg.plugin_prefs.commonOptionsStore.individualDeviceOptions
    )
    if individual_device_options and current_device_config is not None:
        current_backup_config = current_device_config.backupOptionsStore
    else:
        current_backup_config = cfg.plugin_prefs.backupOptionsStore

    supports_series = (
        isinstance(device, KOBOTOUCH)
        and "supports_series" in dir(device)
        and device.supports_series()
    )
    supports_series_list = (
        isinstance(device, KOBOTOUCH)
        and "supports_series_list" in dir(device)
        and device.supports_series_list
    ) or device.dbversion > 136
    supports_ratings = isinstance(device, KOBOTOUCH) and device.dbversion > 36
    try:
        epub_location_like_kepub = (
            isinstance(device, KOBOTOUCH)
            and device.fwversion >= device.min_fwversion_epub_location  # type: ignore[reportOperatorIssue]
        )
    except Exception:
        epub_location_like_kepub = isinstance(
            device, KOBOTOUCH
        ) and device.fwversion >= (4, 17, 13651)  # type: ignore[reportOperatorIssue]

    device_db_path = cast(
        "str", device.normalize_path(device_path + ".kobo/KoboReader.sqlite")
    )
    if isinstance(device, KOBOTOUCH) and hasattr(device, "db_manager"):
        db_path = device.db_manager.dbpath
        is_db_copied = device.db_manager.needs_copy
    else:
        db_path = device_db_path
        is_db_copied = False
    debug("db_path:", db_path)

    timestamp_string = getattr(device, "TIMESTAMP_STRING", "%Y-%m-%dT%H:%M:%SZ")

    kobo_device = KoboDevice(
        device,
        isinstance(device, KOBOTOUCH),
        current_device_profile,
        current_backup_config,
        device_type,
        drive_info,
        device_uuid,
        version_info,
        supports_series,
        supports_series_list,
        supports_ratings,
        epub_location_like_kepub,
        device_name,
        device_path,
        db_path,
        device_db_path,
        is_db_copied,
        timestamp_string,
    )
    debug("kobo_device:", kobo_device)
    return kobo_device
