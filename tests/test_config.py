# ruff: noqa: INP001, PT009
from __future__ import annotations

import copy
import json
import os
import shutil
import sys
import unittest
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path = [TEST_DIR, *sys.path]

CONFIG_SOURCE_PATH = Path(TEST_DIR, "plugin-config.json")
LIBRARY_SOURCE_PATH = Path(TEST_DIR, "library-config.json")

if TYPE_CHECKING:
    from types import TracebackType

    from koboutilities.config import (
        BackupOptionsStoreConfig,
        ConfigDictWrapper,
        DeviceConfig,
        LibraryConfig,
        PicklableJSONConfig,
        PluginConfig,
        ProfileConfig,
        RelatedBooksType,
    )
else:
    from calibre_plugins.koboutilities.config import (
        BackupOptionsStoreConfig,
        ConfigDictWrapper,
        DeviceConfig,
        LibraryConfig,
        PicklableJSONConfig,
        PluginConfig,
        ProfileConfig,
        RelatedBooksType,
    )


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.queue = Queue()
        self.maxDiff = None

    def test_roundtrip(self) -> None:
        with NamedTemporaryFile("w", suffix=".json") as tmp_file:
            tmp_path = Path(tmp_file.name)
            shutil.copy(CONFIG_SOURCE_PATH, tmp_path)
            json_config = PicklableJSONConfig(
                tmp_path.name, base_path=str(tmp_path.parent)
            )
            _ = PluginConfig(json_config)
            self.assertDictEqual(
                json.loads(CONFIG_SOURCE_PATH.read_text()), json_config
            )
            self.assertDictEqual(
                json.loads(CONFIG_SOURCE_PATH.read_text()),
                json.loads(tmp_path.read_text()),
            )

    def test_simple_access(self) -> None:
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config
            self.assertTrue(config.BookmarkOptions.backgroundJob)
            self.assertEqual(config.MetadataOptions.reading_direction, "Default")

    def test_dict_access(self) -> None:
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config
            self.assertEqual(len(config.Devices), 2)
            self.assertIn("N000000000000", config.Devices)
            device = config.Devices["N000000000000"]
            self.assertTrue(device.active)
            self.assertEqual(device.location_code, "main")

    def test_enum_access(self) -> None:
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config
            self.assertEqual(
                config.setRelatedBooksOptionsStore.relatedBooksType,
                RelatedBooksType.Series,
            )

    def test_defaults(self) -> None:
        with PluginConfigManager(None) as plugin_config:
            config = plugin_config.config
            self.assertEqual(config.ReadingOptions.readingFontFamily, "Georgia")
            self.assertEqual(config.ReadingOptions.readingFontSize, 22)
            self.assertFalse(config.ReadingOptions.lockMargins)

    def test_simple_set(self) -> None:
        template_text = "test-template"
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config

            # Set an inner value
            config.MetadataOptions.subtitleTemplate = template_text

            self.assertEqual(config.MetadataOptions.subtitleTemplate, template_text)
            self.assertEqual(
                config.MetadataOptions._wrapped_dict["subtitleTemplate"], template_text
            )

            # Check that the update has been written out correctly
            output = json.loads(plugin_config.path.read_text())
            self.assertEqual(
                output["MetadataOptions"]["subtitleTemplate"], template_text
            )

    def test_set_wrapper(self) -> None:
        dest_dir = "/foo/bar"
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config
            self.assertFalse(config.backupOptionsStore.doDailyBackp)
            self.assertEqual(config.backupOptionsStore.backupDestDirectory, "")

            backup_options = BackupOptionsStoreConfig()
            backup_options.doDailyBackp = True
            backup_options.backupDestDirectory = dest_dir
            config.backupOptionsStore = backup_options

            self.assertTrue(config.backupOptionsStore.doDailyBackp)
            self.assertEqual(config.backupOptionsStore.backupDestDirectory, dest_dir)

            # Check that the options have been written out correctly
            output = json.loads(plugin_config.path.read_text())
            self.assertEqual(
                output["backupOptionsStore"]["backupDestDirectory"], dest_dir
            )

    def test_deepcopy(self) -> None:
        with PluginConfigManager() as plugin_config:
            devices = copy.deepcopy(plugin_config.config.Devices)
            device = devices["N000000000000"]
            self.assertFalse(device.backupOptionsStore.doDailyBackp)
            device.backupOptionsStore.doDailyBackp = True
            self.assertTrue(device.backupOptionsStore.doDailyBackp)
            self.assertTrue(device._wrapped_dict["backupOptionsStore"]["doDailyBackp"])

    def test_set_dict_item(self) -> None:
        serial_no = "N222222222222"
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config

            device = DeviceConfig()
            device.location_code = "test"
            config.Devices[serial_no] = device

            self.assertEqual(len(config.Devices), 3)
            self.assertIn(serial_no, config.Devices)
            device = config.Devices[serial_no]
            self.assertTrue(device.active)
            self.assertEqual(device.location_code, "test")

            # Check that the new device has been written out correctly
            output = json.loads(plugin_config.path.read_text())
            self.assertEqual(output["Devices"][serial_no]["location_code"], "test")

    def test_set_dict(self) -> None:
        serial_no = "N222222222222"
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config

            self.assertEqual(len(config.Devices), 2)
            self.assertNotIn(serial_no, config.Devices)

            new_devices = ConfigDictWrapper()
            self.assertEqual(len(new_devices), 0)
            self.assertNotIn("00000000-0000-0000-0000-000000000000", new_devices)

            device = DeviceConfig()
            device.serial_no = serial_no
            device.location_code = "test"
            new_devices[serial_no] = device
            self.assertNotIn(serial_no, config.Devices)

            config.Devices = new_devices
            self.assertEqual(len(config.Devices), 1)
            self.assertIn(serial_no, config.Devices)

            # Check that the new device dict has been written out correctly
            output = json.loads(plugin_config.path.read_text())
            self.assertEqual(output["Devices"][serial_no]["location_code"], "test")

    def test_contextmanager(self) -> None:
        template_a = "test-a"
        template_b = "test-b"
        template_c = "test-c"

        def check_output_template(template: str) -> None:
            output = json.loads(plugin_config.path.read_text())
            self.assertEqual(output["MetadataOptions"]["subtitleTemplate"], template)

        with PluginConfigManager() as plugin_config:
            plugin_config.config.MetadataOptions.subtitleTemplate = template_a
            check_output_template(template_a)

            # When using config as a context manager it shouldn't commit changes
            # until after the block
            with plugin_config.config as cfg:
                cfg.MetadataOptions.subtitleTemplate = template_b
                check_output_template(template_a)
            check_output_template(template_b)

            metadata_options = plugin_config.config.MetadataOptions
            with metadata_options:
                metadata_options.subtitleTemplate = template_c
                check_output_template(template_b)
            check_output_template(template_c)

    def test_library_config(self) -> None:
        library_dict = json.loads(LIBRARY_SOURCE_PATH.read_text())
        library = LibraryConfig(library_dict)
        self.assertEqual(library.SchemaVersion, 0.1)
        self.assertFalse(library.readingPositionChangesStore.selectBooksInLibrary)
        self.assertEqual(
            library.profiles["Default"].customColumnOptions.lastReadColumn,
            "#kobo_last_read",
        )
        self.assertTrue(library.profiles["Default"].storeOptionsStore.promptToStore)

    def test_library_roundtrip(self) -> None:
        library_dict = json.loads(LIBRARY_SOURCE_PATH.read_text())
        library_dict_copy = copy.deepcopy(library_dict)
        library = LibraryConfig(library_dict)
        self.assertDictEqual(library_dict_copy, library._wrapped_dict)

    def test_library_defaults(self) -> None:
        library = LibraryConfig({})
        self.assertEqual(library.SchemaVersion, 0.1)
        self.assertFalse(library.readingPositionChangesStore.selectBooksInLibrary)

    def test_new_profile(self) -> None:
        library_dict = json.loads(LIBRARY_SOURCE_PATH.read_text())
        library = LibraryConfig(library_dict)

        profile_name = "test-profile"
        profile = ProfileConfig()
        profile.forDevice = "test-device"
        profile.storeOptionsStore.promptToStore = False
        library.profiles[profile_name] = profile

        self.assertEqual(len(library.profiles), 2)
        self.assertIn(profile_name, library.profiles)
        profile = library.profiles[profile_name]
        self.assertEqual(profile.forDevice, "test-device")
        self.assertFalse(profile.storeOptionsStore.promptToStore)

    def test_iter(self) -> None:
        serial_no = "N000000000000"
        with PluginConfigManager() as plugin_config:
            config = plugin_config.config
            device = config.Devices[serial_no]
            self.assertEqual(len(list(device)), 7)
            device_dict = dict(device)
            self.assertTrue(device_dict.pop("active"))
            self.assertEqual(device_dict.pop("location_code"), "main")
            self.assertEqual(device_dict.pop("name"), "Kobo Test 0")
            self.assertEqual(device_dict.pop("serial_no"), serial_no)
            self.assertEqual(device_dict.pop("type"), "Kobo Test 0")
            self.assertEqual(
                device_dict.pop("uuid"), "00000000-0000-0000-0000-000000000000"
            )
            backup_store = device_dict.pop("backupOptionsStore")
            self.assertIsInstance(backup_store, BackupOptionsStoreConfig)
            self.assertEqual(len(device_dict), 0)


@dataclass
class PluginConfigWrapper:
    config: PluginConfig
    path: Path


class PluginConfigManager:
    def __init__(self, config_path: Path | None = CONFIG_SOURCE_PATH):
        self.tmp_file_ctx = None
        self.config_path = config_path

    def __enter__(self):
        self.tmp_file_ctx = NamedTemporaryFile("w", suffix=".json")
        tmp_file = self.tmp_file_ctx.__enter__()
        tmp_path = Path(tmp_file.name)
        if self.config_path is not None:
            shutil.copy(self.config_path, tmp_path)
        json_config = PicklableJSONConfig(tmp_path.name, base_path=str(tmp_path.parent))
        return PluginConfigWrapper(PluginConfig(json_config), tmp_path)

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ):
        if self.tmp_file_ctx is not None:
            return self.tmp_file_ctx.__exit__(exc, value, tb)
        return False


if __name__ == "__main__":
    unittest.main(module=Path(__file__).stem, verbosity=2)
