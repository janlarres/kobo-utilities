from __future__ import annotations

load_translations()

GUI_NAME = _("Kobo Utilities")
BOOK_CONTENTTYPE = 6
MIMETYPE_KOBO = "application/x-kobo-epub+zip"
WIKI_URL = "https://github.com/janlarres/kobo-utilities/wiki"
CORRUPT_MSG = _("""
    The database on your device is corrupt, and Kobo Utilities is unable to continue with its current task. You have two options:
    <p>
    <ul>
    <li>If you have a backup made by Kobo Utilities, restore it to your device.
    <li>Re-login to your account on your device to repair it. Note that this will likely result in you losing your reading progress and annotations. You can then use Kobo Utilities to restore your reading progress from the Calibre library.
    </ul>
""")
