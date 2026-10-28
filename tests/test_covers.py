# ruff: noqa: PT009
from __future__ import annotations

import os
import sys
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from calibre.devices.kobo.books import Book
from calibre.devices.kobo.driver import KOBOTOUCH
from calibre.ebooks.metadata import MetaInformation

test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path = [test_dir, *sys.path]

if TYPE_CHECKING:
    from koboutilities import config, utils
    from koboutilities.features import covers
else:
    from calibre_plugins.koboutilities import config, utils
    from calibre_plugins.koboutilities.features import covers


class TestCovers(unittest.TestCase):
    def setUp(self):
        self.driver = mock.MagicMock(spec=KOBOTOUCH)
        self.driver.normalize_path.side_effect = lambda path: path
        fwversion = (4, 41, 23145)
        self.device = config.KoboDevice(
            driver=self.driver,
            is_kobotouch=True,
            profile=None,
            backup_config=config.BackupOptionsStoreConfig(),
            device_type="",
            drive_info={},
            uuid="1234",
            version_info=config.KoboVersionInfo("1", fwversion, "1"),
            supports_series=True,
            supports_series_list=True,
            supports_ratings=True,
            epub_location_like_kepub=False,
            name="test-device",
            path="/",
            db_path="/kobo.sqlite",
            device_db_path="/kobo.sqlite",
            is_db_copied=False,
            timestamp_string=KOBOTOUCH.TIMESTAMP_STRING,
        )

    def test_upload_covers(self):
        mi = MetaInformation("Title", ["Author"])
        book = Book("", "lpath", title=mi.title, other=mi)
        book.calibre_id = 1
        book_path = "/Author/Title - Author.epub"

        options = config.CoverUploadConfig()
        options.blackandwhite = True
        options.dithered_covers = True
        options.keep_cover_aspect = True
        options.letterbox = True
        options.letterbox_color = "#123456"
        options.png_covers = True

        with mock.patch.object(
            utils, "get_device_paths_from_id", return_value=[book_path]
        ):
            total_books, uploaded_covers, not_on_device_books = covers._upload_covers(
                [book], self.device, mock.MagicMock(), options
            )

        self.assertEqual((total_books, uploaded_covers, not_on_device_books), (1, 1, 0))
        self.driver._upload_cover.assert_called_once_with(
            book_path,
            "",
            book,
            book_path,
            True,
            dithered_covers=True,
            keep_cover_aspect=True,
            letterbox_fs_covers=True,
            letterbox_color="#123456",
            png_covers=True,
        )
