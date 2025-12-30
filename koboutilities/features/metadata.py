from __future__ import annotations

import datetime as dt
import os
import time
from typing import TYPE_CHECKING, cast

from calibre import strftime
from calibre.constants import DEBUG
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.dialogs.template_dialog import TemplateDialog
from qt.core import (
    QCheckBox,
    QComboBox,
    QDialogButtonBox,
    QGridLayout,
    QGroupBox,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from .. import config as cfg
from .. import utils
from ..constants import BOOK_CONTENTTYPE, GUI_NAME
from ..dialogs import (
    CustomColumnComboBox,
    ImageTitleLayout,
    PluginDialog,
    ProgressBar,
    ReadingStatusGroupBox,
)
from ..utils import debug

if TYPE_CHECKING:
    from calibre.devices.kobo.books import Book
    from calibre.ebooks.metadata.book.base import Metadata
    from calibre.gui2 import ui

    from ..config import KoboDevice
    from ..utils import Dispatcher, LoadResources

DATE_COLUMNS = [
    "timestamp",
    "last_modified",
    "pubdate",
]

READING_DIRECTIONS = {
    _("Default"): "default",
    _("RTL"): "rtl",
    _("LTR"): "ltr",
}


def update_metadata(
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

    progressbar = ProgressBar(parent=gui, window_title=_("Getting book list"))
    progressbar.set_label(_("Number of selected books: {0}").format(len(selectedIDs)))
    progressbar.show_with_maximum(len(selectedIDs))
    debug("selectedIDs:", selectedIDs)
    books = utils.convert_calibre_ids_to_books(current_view.model().db, selectedIDs)
    for book in books:
        progressbar.increment()
        device_book_paths = utils.get_device_paths_from_id(
            cast("int", book.calibre_id), gui
        )
        debug("device_book_paths:", device_book_paths)
        book.paths = device_book_paths
        book.contentIDs = [
            utils.contentid_from_path(device, path, BOOK_CONTENTTYPE)
            for path in device_book_paths
        ]
    progressbar.hide()

    dlg = UpdateMetadataOptionsDialog(gui, device, books[0])
    dlg.exec()
    if dlg.result() != dlg.DialogCode.Accepted:
        return

    progressbar = ProgressBar(parent=gui, window_title=_("Updating metadata on device"))
    progressbar.show()
    progressbar.set_label(
        _("Number of books to update metadata for: {0}").format(len(books))
    )
    options = cfg.plugin_prefs.MetadataOptions
    updated_books, unchanged_books, not_on_device_books, count_books = (
        do_update_metadata(books, device, gui, progressbar, options)
    )
    result_message = (
        _("Update summary:")
        + "\n\t"
        + _(
            "Books updated={0}\n\tUnchanged books={1}\n\tBooks not on device={2}\n\tTotal books={3}"
        ).format(updated_books, unchanged_books, not_on_device_books, count_books)
    )
    info_dialog(
        gui,
        _("Kobo Utilities") + " - " + _("Device library updated"),
        result_message,
        show=True,
    )


def do_update_metadata(
    books: list[Book],
    device: KoboDevice,
    gui: ui.Main,
    progressbar: ProgressBar,
    options: cfg.MetadataOptionsConfig,
):
    from calibre.ebooks.metadata import authors_to_string
    from calibre.utils.localization import canonicalize_lang, lang_as_iso639_1

    debug("number books=", len(books), "options=", options)

    updated_books = 0
    not_on_device_books = 0
    unchanged_books = 0
    count_books = 0

    total_books = len(books)
    progressbar.show_with_maximum(total_books)

    from calibre.library.save_to_disk import find_plugboard

    plugboards = gui.library_view.model().db.prefs.get("plugboards", {})
    debug("plugboards=", plugboards)
    debug(
        "self.device.driver.__class__.__name__=",
        device.driver.__class__.__name__,
    )

    rating_update = (
        "UPDATE ratings "
        "SET Rating = ?, "
        "DateModified = ? "
        "WHERE ContentID  = ?"
    )  # fmt: skip
    rating_insert = (
        "INSERT INTO ratings ("
        "Rating, "
        "DateModified, "
        "ContentID "
        ")"
        "VALUES (?, ?, ?)"
    )  # fmt: skip
    rating_delete = "DELETE FROM ratings WHERE ContentID = ?"

    series_id_query = (
        "SELECT DISTINCT Series, SeriesID "
        "FROM content "
        "WHERE contentType = 6 "
        "AND contentId NOT LIKE 'file%' "
        "AND series IS NOT NULL "
        "AND seriesid IS NOT NULL "
    )

    with utils.device_database_connection(device, use_row_factory=True) as connection:
        test_query = generate_metadata_query(device)
        cursor = connection.cursor()
        kobo_series_dict = {}
        if device.supports_series_list:
            cursor.execute(series_id_query)
            rows = list(cursor)
            debug("series_id_query result=", rows)
            for row in rows:
                kobo_series_dict[row["Series"]] = row["SeriesID"]
            debug("kobo_series_list=", kobo_series_dict)

        for book in books:
            progressbar.set_label(_("Updating metadata for {}").format(book.title))
            progressbar.increment()

            for contentID in cast("list[str]", book.contentIDs):
                debug("searching for contentId='%s'" % (contentID))
                if not contentID:
                    contentID = utils.contentid_from_path(
                        device, book.path, BOOK_CONTENTTYPE
                    )
                debug("options.update_KoboEpubs=", options.update_KoboEpubs)
                debug("contentID.startswith('file')=", contentID.startswith("file"))
                if not options.update_KoboEpubs and not contentID.startswith("file"):
                    debug("skipping book")
                    continue

                count_books += 1
                query_values = (contentID,)
                cursor.execute(test_query, query_values)
                try:
                    result = next(cursor)
                except StopIteration:
                    result = None
                if result is not None:
                    debug("found contentId='%s'" % (contentID))
                    debug("    result=", result)
                    debug("    result['Title']='%s'" % (result["Title"]))
                    debug("    result['Attribution']='%s'" % (result["Attribution"]))

                    title_string = None
                    authors_string = None
                    newmi = book.deepcopy_metadata()
                    if options.usePlugboard and plugboards is not None:
                        book_format = os.path.splitext(contentID)[1][1:]
                        debug("format='%s'" % (book_format))
                        plugboard = find_plugboard(
                            device.driver.__class__.__name__,
                            book_format,
                            plugboards,
                        )
                        debug("plugboard=", plugboard)

                        if plugboard is not None:
                            debug("applying plugboard")
                            newmi.template_to_attribute(book, plugboard)
                        debug("newmi.title=", newmi.title)
                        debug("newmi.authors=", newmi.authors)
                        debug("newmi.comments=", newmi.comments)
                    else:
                        if options.titleSort:
                            title_string = newmi.title_sort
                        if options.authourSort:
                            debug("author=", newmi.authors)
                            debug("using author_sort=", newmi.author_sort)
                            debug("using author_sort - author=", newmi.authors)
                            authors_string = newmi.author_sort
                    debug("title_string=", title_string)
                    title_string = newmi.title if title_string is None else title_string
                    debug("title_string=", title_string)
                    debug("authors_string=", authors_string)
                    authors_string = (
                        authors_to_string(newmi.authors)
                        if authors_string is None
                        else authors_string
                    )
                    debug("authors_string=", authors_string)
                    newmi.series_index_string = getattr(
                        book, "series_index_string", None
                    )

                    update_query = "UPDATE content SET "
                    update_values = []
                    set_clause_columns = []
                    changes_found = False
                    rating_values = []
                    rating_change_query = None

                    if options.title and result["Title"] != title_string:
                        set_clause_columns.append("Title=?")
                        debug("set_clause=", set_clause_columns)
                        update_values.append(title_string)

                    if options.author and result["Attribution"] != authors_string:
                        set_clause_columns.append("Attribution=?")
                        debug("set_clause_columns=", set_clause_columns)
                        update_values.append(authors_string)

                    if options.description:
                        new_comments = library_comments = newmi.comments
                        if options.descriptionUseTemplate:
                            new_comments = _render_synopsis(
                                newmi, book, template=options.descriptionTemplate
                            )
                            if len(new_comments) == 0:
                                new_comments = library_comments
                        if (
                            new_comments
                            and len(new_comments) > 0
                            and result["Description"] != new_comments
                        ):
                            set_clause_columns.append("Description=?")
                            update_values.append(new_comments)
                        else:
                            debug("Description not changed - not updating.")

                    if options.publisher and result["Publisher"] != newmi.publisher:
                        set_clause_columns.append("Publisher=?")
                        update_values.append(newmi.publisher)

                    if options.published_date:
                        pubdate_string = strftime(
                            device.timestamp_string, newmi.pubdate
                        )
                        if result["DateCreated"] != pubdate_string:
                            set_clause_columns.append("DateCreated=?")
                            debug(
                                "convert_kobo_date(result['DateCreated'])=",
                                utils.convert_kobo_date(result["DateCreated"]),
                            )
                            debug("newmi.pubdate  =", newmi.pubdate)
                            debug("result['DateCreated']     =", result["DateCreated"])
                            debug("pubdate_string=", pubdate_string)
                            debug("newmi.pubdate.__class__=", newmi.pubdate.__class__)
                            update_values.append(pubdate_string)

                    if options.isbn and result["ISBN"] != newmi.isbn:
                        set_clause_columns.append("ISBN=?")
                        update_values.append(newmi.isbn)

                    if options.language and result["Language"] != lang_as_iso639_1(
                        newmi.language
                    ):
                        debug("newmi.language =", newmi.language)
                        debug(
                            "lang_as_iso639_1(newmi.language)=",
                            lang_as_iso639_1(newmi.language),
                        )
                        debug(
                            "canonicalize_lang(newmi.language)=",
                            canonicalize_lang(newmi.language),
                        )

                    debug("options.rating= ", options.rating)
                    if options.rating:
                        rating_column = cfg.get_column_names(gui, device).rating

                        if rating_column:
                            if rating_column == "rating":
                                rating = newmi.rating
                            else:
                                metadata = newmi.get_user_metadata(rating_column, True)
                                assert metadata is not None
                                rating = metadata["#value#"]
                            debug(
                                "rating=",
                                rating,
                                "result[Rating]=",
                                result["Rating"],
                            )
                            assert isinstance(rating, int)
                            rating = None if not rating or rating == 0 else rating / 2
                            debug(
                                "rating=",
                                rating,
                                "result[Rating]=",
                                result["Rating"],
                            )
                            rating_values.append(rating)
                            rating_values.append(
                                strftime(device.timestamp_string, time.gmtime())
                            )
                            rating_values.append(contentID)
                            if rating != result["Rating"]:
                                if not rating:
                                    rating_change_query = rating_delete
                                    rating_values = (contentID,)
                                elif (
                                    result["DateModified"] is None
                                ):  # If the date modified column does not have a value, there is no rating column
                                    rating_change_query = rating_insert
                                else:
                                    rating_change_query = rating_update

                    debug("options.series=", options.series)
                    if device.supports_series and options.series:
                        debug(
                            "newmi.series= ='%s' newmi.series_index='%s' newmi.series_index_string='%s'"
                            % (
                                newmi.series,
                                newmi.series_index,
                                newmi.series_index_string,
                            )
                        )
                        debug(
                            "result['Series'] ='%s' result['SeriesNumber'] =%s"
                            % (result["Series"], result["SeriesNumber"])
                        )
                        debug(
                            "result['SeriesID'] ='%s' result['SeriesNumberFloat'] =%s"
                            % (result["SeriesID"], result["SeriesNumberFloat"])
                        )

                        if newmi.series is not None:
                            new_series = newmi.series
                            try:
                                new_series_number = "%g" % newmi.series_index
                            except Exception:
                                new_series_number = None
                        else:
                            new_series = None
                            new_series_number = None

                        series_changed = new_series != result["Series"]
                        series_number_changed = (
                            new_series_number != result["SeriesNumber"]
                        )
                        debug('new_series="%s"' % (new_series,))
                        debug('new_series_number="%s"' % (new_series_number,))
                        debug('series_number_changed="%s"' % (series_number_changed,))
                        debug('series_changed="%s"' % (series_changed,))
                        if series_changed or series_number_changed:
                            debug("setting series")
                            set_clause_columns.append("Series=?")
                            update_values.append(new_series)
                            set_clause_columns.append("SeriesNumber=?")
                            update_values.append(new_series_number)
                        debug(
                            "self.device.supports_series_list='%s'"
                            % device.supports_series_list
                        )
                        if device.supports_series_list:
                            debug("supports_series_list")
                            series_id = kobo_series_dict.get(newmi.series, newmi.series)
                            debug("series_id='%s'" % series_id)
                            if (
                                series_changed
                                or series_number_changed
                                or not (
                                    result["SeriesID"] == series_id
                                    and (
                                        result["SeriesNumberFloat"]
                                        == newmi.series_index
                                    )
                                )
                            ):
                                debug("setting SeriesID")
                                set_clause_columns.append("SeriesID=?")
                                set_clause_columns.append("SeriesNumberFloat=?")
                                if series_id is None or series_id == "":
                                    update_values.append(None)
                                    update_values.append(None)
                                else:
                                    update_values.append(series_id)
                                    update_values.append(newmi.series_index)

                    if options.subtitle:
                        debug(
                            "setting subtitle - column name =",
                            options.subtitleTemplate,
                        )
                        subtitle_template = options.subtitleTemplate
                        if subtitle_template == cfg.TOKEN_CLEAR_SUBTITLE:
                            new_subtitle = None
                        elif subtitle_template and subtitle_template[0] == "#":
                            metadata = newmi.get_user_metadata(subtitle_template, True)
                            assert metadata is not None
                            new_subtitle = metadata["#value#"]
                        else:
                            pb = [
                                (
                                    subtitle_template,
                                    "subtitle",
                                )
                            ]
                            book.template_to_attribute(book, pb)
                            debug("after - mi.subtitle=", book.subtitle)
                            assert book.subtitle is not None
                            new_subtitle = (
                                book.subtitle if len(book.subtitle) > 0 else None
                            )
                            if new_subtitle and subtitle_template == new_subtitle:
                                new_subtitle = None
                            debug('setting subtitle - subtitle ="%s"' % new_subtitle)
                            debug(
                                'setting subtitle - result["Subtitle"] = "%s"'
                                % result["Subtitle"]
                            )
                        if (
                            not new_subtitle
                            and (
                                not (
                                    result["Subtitle"] is None
                                    or result["Subtitle"] == ""
                                )
                            )
                        ) or (new_subtitle and result["Subtitle"] != new_subtitle):
                            update_values.append(new_subtitle)
                            set_clause_columns.append("Subtitle=?")

                    debug(
                        "options.set_reading_direction",
                        options.set_reading_direction,
                    )
                    debug("options.reading_direction", options.reading_direction)
                    if options.set_reading_direction and (
                        result["PageProgressDirection"] != options.reading_direction
                    ):
                        set_clause_columns.append("PageProgressDirection=?")
                        update_values.append(options.reading_direction)

                    debug("options.set_sync_date", options.set_sync_date)
                    debug(
                        "options.sync_date_library_date",
                        options.sync_date_library_date,
                    )
                    new_timestamp = None
                    if options.set_sync_date:
                        if options.sync_date_library_date == "timestamp":
                            new_timestamp = newmi.timestamp
                        elif options.sync_date_library_date == "last_modified":
                            new_timestamp = newmi.last_modified
                        elif options.sync_date_library_date == "pubdate":
                            new_timestamp = newmi.pubdate
                        elif options.sync_date_library_date[0] == "#":
                            metadata = newmi.get_user_metadata(
                                options.sync_date_library_date, True
                            )
                            assert metadata is not None
                            new_timestamp = metadata["#value#"]
                        elif options.sync_date_library_date == cfg.TOKEN_FILE_TIMESTAMP:
                            debug("Using book file timestamp for Date Added sort.")
                            debug("book=", book)
                            device_book_path = utils.get_device_path_from_contentID(
                                device, contentID, result["MimeType"]
                            )
                            debug("device_book_path=", device_book_path)
                            new_timestamp = dt.datetime.fromtimestamp(
                                os.path.getmtime(device_book_path),
                                tz=dt.timezone.utc,
                            )
                            debug("new_timestamp=", new_timestamp)

                        if new_timestamp is not None:
                            synctime_string = strftime(
                                device.timestamp_string, new_timestamp
                            )
                            if result["___SyncTime"] != synctime_string:
                                set_clause_columns.append("___SyncTime=?")
                                debug(
                                    "convert_kobo_date(result['___SyncTime'])=",
                                    utils.convert_kobo_date(result["___SyncTime"]),
                                )
                                debug(
                                    "convert_kobo_date(result['___SyncTime']).__class__=",
                                    utils.convert_kobo_date(
                                        result["___SyncTime"]
                                    ).__class__,
                                )
                                debug("new_timestamp  =", new_timestamp)
                                debug(
                                    "result['___SyncTime']     =",
                                    result["___SyncTime"],
                                )
                                debug("synctime_string=", synctime_string)
                                update_values.append(synctime_string)

                    if options.setRreadingStatus and (
                        result["ReadStatus"] != options.readingStatus
                        or options.resetPosition
                    ):
                        set_clause_columns.append("ReadStatus=?")
                        update_values.append(options.readingStatus)
                        if options.resetPosition:
                            set_clause_columns.append("DateLastRead=?")
                            update_values.append(None)
                            set_clause_columns.append("ChapterIDBookmarked=?")
                            update_values.append(None)
                            set_clause_columns.append("___PercentRead=?")
                            update_values.append(0)
                            set_clause_columns.append("FirstTimeReading=?")
                            update_values.append(options.readingStatus < 2)

                    if len(set_clause_columns) > 0:
                        update_query += ",".join(set_clause_columns)
                        changes_found = True

                    if not (changes_found or rating_change_query):
                        debug(
                            "no changes found to selected metadata. No changes being made."
                        )
                        unchanged_books += 1
                        continue

                    update_query += " WHERE ContentID = ? AND BookID IS NULL"
                    update_values.append(contentID)
                    debug("update_query=%s" % update_query)
                    debug("update_values= ", update_values)
                    try:
                        if changes_found:
                            cursor.execute(update_query, update_values)

                        if rating_change_query:
                            debug("rating_change_query=%s" % rating_change_query)
                            debug("rating_values= ", rating_values)
                            cursor.execute(rating_change_query, rating_values)

                        updated_books += 1
                    except:
                        debug("    Database Exception:  Unable to set series info")
                        raise
                else:
                    debug(
                        "no match for title='%s' contentId='%s'"
                        % (book.title, contentID)
                    )
                    not_on_device_books += 1
    debug(
        "Update summary: Books updated=%d, unchanged books=%d, not on device=%d, Total=%d"
        % (updated_books, unchanged_books, not_on_device_books, count_books)
    )

    progressbar.hide()

    return (updated_books, unchanged_books, not_on_device_books, count_books)


def generate_metadata_query(device: KoboDevice):
    debug(
        "self.device.supports_series=%s, self.device.supports_series_list%s"
        % (device.supports_series, device.supports_series_list)
    )

    test_query_columns = []
    test_query_columns.append("Title")
    test_query_columns.append("Attribution")
    test_query_columns.append("Description")
    test_query_columns.append("Publisher")
    test_query_columns.append("MimeType")

    if device.supports_series:
        debug("supports series is true")
        test_query_columns.append("Series")
        test_query_columns.append("SeriesNumber")
        test_query_columns.append("Subtitle")
    else:
        test_query_columns.append("null as Series")
        test_query_columns.append("null as SeriesNumber")
    if device.supports_series_list:
        debug("supports series list is true")
        test_query_columns.append("SeriesID")
        test_query_columns.append("SeriesNumberFloat")
    else:
        test_query_columns.append("null as SeriesID")
        test_query_columns.append("null as SeriesNumberFloat")

    test_query_columns.append("ReadStatus")
    test_query_columns.append("DateCreated")
    test_query_columns.append("Language")
    test_query_columns.append("PageProgressDirection")
    test_query_columns.append("___SyncTime")
    if device.supports_ratings:
        test_query_columns.append("ISBN")
        test_query_columns.append("FeedbackType")
        test_query_columns.append("FeedbackTypeSynced")
        test_query_columns.append("r.Rating")
        test_query_columns.append("r.DateModified")
    else:
        test_query_columns.append("NULL as ISBN")
        test_query_columns.append("NULL as FeedbackType")
        test_query_columns.append("NULL as FeedbackTypeSynced")
        test_query_columns.append("NULL as Rating")
        test_query_columns.append("NULL as DateModified")

    test_query = "SELECT "
    test_query += ",".join(test_query_columns)
    test_query += " FROM content c1 "
    if device.supports_ratings:
        test_query += " left outer join ratings r on c1.ContentID = r.ContentID "

    test_query += "WHERE c1.BookId IS NULL AND c1.ContentID = ?"
    debug("test_query=%s" % test_query)
    return test_query


def _render_synopsis(mi: Metadata, book: Book, template: str | None = None):
    from xml.sax.saxutils import escape

    from calibre.customize.ui import output_profiles
    from calibre.ebooks.conversion.config import load_defaults
    from calibre.ebooks.oeb.transforms.jacket import (
        SafeFormatter,
        Series,
        Tags,
        get_rating,
    )
    from calibre.library.comments import comments_to_html
    from calibre.utils.date import is_date_undefined

    debug('start - book.comments="%s"' % book.comments)

    if not template:
        try:
            data = P("kobo_template.xhtml", data=True)
            assert isinstance(data, bytes), f"data is of type {type(data)}"
            template = data.decode("utf-8")
        except Exception:
            template = ""
    debug("template=", template)

    colon_pos = template.find(":")
    jacket_style = False
    if colon_pos > 0:
        if template.startswith(("template:", "plugboard:")):
            jacket_style = False
            template = template[colon_pos + 1 :]
        elif template.startswith("jacket:"):
            jacket_style = True
            template = template[colon_pos + 1 :]

    if jacket_style:
        debug("using jacket style template.")

        ps = load_defaults("page_setup")
        op = ps.get("output_profile", "default")
        opmap = {x.short_name: x for x in output_profiles()}
        output_profile = opmap.get(op, opmap["default"])

        rating = get_rating(
            mi.rating,
            output_profile.ratings_char,
            output_profile.empty_ratings_char,
        )

        tags = Tags((mi.tags if mi.tags else []), output_profile)
        debug("tags=", tags)

        comments = mi.comments.strip() if mi.comments else ""
        if comments:
            comments = comments_to_html(comments)
        debug("comments=", comments)
        try:
            author = mi.format_authors()
        except Exception:
            author = ""
        author = escape(author)
        publisher = cast("str", mi.publisher) if mi.publisher else ""
        publisher = escape(publisher)
        title_str = mi.title if mi.title else _("Unknown")
        title_str = escape(title_str)
        series = Series(mi.series, mi.series_index)

        try:
            if is_date_undefined(mi.pubdate):
                pubdate = ""
            else:
                pubdate = strftime("%Y", cast("dt.date", mi.pubdate).timetuple())
        except Exception:
            pubdate = ""

        args = {
            "title_str": title_str,
            "title": title_str,
            "author": author,
            "publisher": publisher,
            "pubdate_label": _("Published"),
            "pubdate": pubdate,
            "series_label": _("Series"),
            "series": series,
            "rating_label": _("Rating"),
            "rating": rating,
            "tags_label": _("Tags"),
            "tags": tags,
            "comments": comments,
        }
        for key in mi.custom_field_keys():
            try:
                display_name, val = mi.format_field_extended(key)[:2]
                debug("key=%s, display_name=%s, val=%s" % (key, display_name, val))
                key = key.replace("#", "_")
                args[key + "_label"] = escape(display_name)
                debug("display_name arg=", (args[key + "_label"]))
                args[key] = escape(val)
            except Exception:  # noqa: PERF203, S110
                # if the val (custom column contents) is None, don't add to args
                pass

        if DEBUG:
            debug("Custom column values available in jacket template:")
            for key in list(args.keys()):
                if key.startswith("_") and not key.endswith("_label"):
                    debug(" %s: %s" % ("#" + key[1:], args[key]))

        # Used in the comment describing use of custom columns in templates
        # Don't change this unless you also change it in template.xhtml
        args["_genre_label"] = args.get("_genre_label", "{_genre_label}")
        args["_genre"] = args.get("_genre", "{_genre}")

        formatter = SafeFormatter()
        rendered_comments = formatter.format(template, **args)
        debug("generated_html=", rendered_comments)

    else:
        pb = [(template, "comments")]
        debug("before - mi.comments=", mi.comments)
        debug("book.comments=", book.comments)
        debug("pb=", pb)
        mi.template_to_attribute(book, pb)
        debug("after - mi.comments=", mi.comments)
        rendered_comments = mi.comments

    return rendered_comments


class UpdateMetadataOptionsDialog(PluginDialog):
    def __init__(
        self,
        parent: ui.Main,
        device: KoboDevice,
        book: Book,
    ):
        super().__init__(
            parent,
            "kobo utilities plugin:update metadata settings dialog",
        )
        self.gui = parent
        self.test_book = book

        self.initialize_controls()

        # Set some default values from last time dialog was used.
        title = cfg.plugin_prefs.MetadataOptions.title
        self.title_checkbox.setChecked(title)
        self.update_title_sort_checkbox()

        title_sort = cfg.plugin_prefs.MetadataOptions.titleSort
        self.title_sort_checkbox.setChecked(title_sort)

        author = cfg.plugin_prefs.MetadataOptions.author
        self.author_checkbox.setChecked(author)

        author_sort = cfg.plugin_prefs.MetadataOptions.authourSort
        self.author_sort_checkbox.setChecked(author_sort)
        self.update_author_sort_checkbox()

        description = cfg.plugin_prefs.MetadataOptions.description
        self.description_checkbox.setChecked(description)

        description_use_template = (
            cfg.plugin_prefs.MetadataOptions.descriptionUseTemplate
        )
        self.description_use_template_checkbox.setChecked(description_use_template)
        self.description_checkbox_clicked(description)
        description_template = cfg.plugin_prefs.MetadataOptions.descriptionTemplate
        self.description_template_edit.template = description_template

        publisher = cfg.plugin_prefs.MetadataOptions.publisher
        self.publisher_checkbox.setChecked(publisher)

        published = cfg.plugin_prefs.MetadataOptions.published_date
        self.published_checkbox.setChecked(published)

        isbn = cfg.plugin_prefs.MetadataOptions.isbn
        supports_ratings = device.supports_ratings
        self.isbn_checkbox.setChecked(isbn and supports_ratings)
        self.isbn_checkbox.setEnabled(supports_ratings)

        rating = cfg.plugin_prefs.MetadataOptions.rating
        self.rating_checkbox.setChecked(rating and supports_ratings)
        has_rating_column = cfg.get_column_names(self.gui, device).rating != ""
        self.rating_checkbox.setEnabled(has_rating_column and supports_ratings)

        series = cfg.plugin_prefs.MetadataOptions.series
        self.series_checkbox.setChecked(series and device.supports_series)
        self.series_checkbox.setEnabled(device.supports_series)

        subtitle = cfg.plugin_prefs.MetadataOptions.subtitle
        self.subtitle_checkbox.setChecked(subtitle)
        self.subtitle_checkbox_clicked(subtitle)

        subtitle_template = cfg.plugin_prefs.MetadataOptions.subtitleTemplate
        self.subtitle_template_edit.template = subtitle_template

        reading_direction = cfg.plugin_prefs.MetadataOptions.set_reading_direction
        self.reading_direction_checkbox.setChecked(reading_direction)
        self.reading_direction_checkbox_clicked(reading_direction)
        reading_direction = cfg.plugin_prefs.MetadataOptions.reading_direction
        self.reading_direction_combo.select_text(reading_direction)

        date_added = cfg.plugin_prefs.MetadataOptions.set_sync_date
        self.date_added_checkbox.setChecked(date_added)
        date_added_column = cfg.plugin_prefs.MetadataOptions.sync_date_library_date
        self.date_added_column_combo.populate_combo(
            self.get_date_columns(DATE_COLUMNS),
            date_added_column,
            initial_items=cfg.OTHER_SORTS,
            show_lookup_name=False,
        )
        self.date_added_checkbox_clicked(date_added)

        use_plugboard = cfg.plugin_prefs.MetadataOptions.usePlugboard
        self.use_plugboard_checkbox.setChecked(use_plugboard)
        self.use_plugboard_checkbox_clicked()

        update_kepubs = cfg.plugin_prefs.MetadataOptions.update_KoboEpubs
        self.update_kepubs_checkbox.setChecked(update_kepubs)

        language = cfg.plugin_prefs.MetadataOptions.language
        self.language_checkbox.setChecked(language)

        # Cause our dialog size to be restored from prefs or created on first usage
        self.resize_dialog()

    def initialize_controls(self):
        self.setWindowTitle(GUI_NAME)
        layout = QVBoxLayout(self)
        self.setLayout(layout)
        title_layout = ImageTitleLayout(
            self,
            "images/icon.png",
            _("Update metadata in device library"),
            "UpdateMetadata",
        )
        layout.addLayout(title_layout)

        options_group = QGroupBox(_("Metadata to update"), self)
        layout.addWidget(options_group)
        options_layout = QGridLayout()
        options_group.setLayout(options_layout)

        widget_line = 0
        self.title_checkbox = QCheckBox(_("Title"), self)
        options_layout.addWidget(self.title_checkbox, widget_line, 0, 1, 1)
        self.title_checkbox.clicked.connect(self.update_title_sort_checkbox)
        self.title_sort_checkbox = QCheckBox(_("Use 'Title Sort'"), self)
        options_layout.addWidget(self.title_sort_checkbox, widget_line, 1, 1, 1)

        self.author_checkbox = QCheckBox(_("Author"), self)
        options_layout.addWidget(self.author_checkbox, widget_line, 2, 1, 1)
        self.author_checkbox.clicked.connect(self.update_author_sort_checkbox)
        self.author_sort_checkbox = QCheckBox(_("Use 'Author Sort'"), self)
        options_layout.addWidget(self.author_sort_checkbox, widget_line, 3, 1, 1)

        widget_line += 1
        self.description_checkbox = QCheckBox(_("Comments/Synopsis"), self)
        options_layout.addWidget(self.description_checkbox, 1, 0, 1, 1)
        self.description_checkbox.clicked.connect(self.description_checkbox_clicked)
        self.description_use_template_checkbox = QCheckBox(_("Use template"), self)
        options_layout.addWidget(
            self.description_use_template_checkbox, widget_line, 1, 1, 1
        )
        self.description_use_template_checkbox.clicked.connect(
            self.description_use_template_checkbox_clicked
        )

        self.description_template_edit = TemplateConfig(mi=self.test_book)
        description_template_edit_tooltip = _(
            "Enter a template to use to set the comment/synopsis."
        )
        self.description_template_edit.setToolTip(description_template_edit_tooltip)
        options_layout.addWidget(self.description_template_edit, widget_line, 2, 1, 2)

        widget_line += 1
        self.series_checkbox = QCheckBox(_("Series and index"), self)
        options_layout.addWidget(self.series_checkbox, widget_line, 0, 1, 2)

        self.publisher_checkbox = QCheckBox(_("Publisher"), self)
        options_layout.addWidget(self.publisher_checkbox, widget_line, 2, 1, 2)

        widget_line += 1
        self.published_checkbox = QCheckBox(_("Published date"), self)
        options_layout.addWidget(self.published_checkbox, widget_line, 0, 1, 2)

        self.isbn_checkbox = QCheckBox(_("ISBN"), self)
        options_layout.addWidget(self.isbn_checkbox, widget_line, 2, 1, 2)

        widget_line += 1
        self.language_checkbox = QCheckBox(_("Language"), self)
        options_layout.addWidget(self.language_checkbox, widget_line, 0, 1, 2)

        self.rating_checkbox = QCheckBox(_("Rating"), self)
        options_layout.addWidget(self.rating_checkbox, widget_line, 2, 1, 2)

        widget_line += 1
        self.subtitle_checkbox = QCheckBox(_("Subtitle"), self)
        options_layout.addWidget(self.subtitle_checkbox, widget_line, 0, 1, 2)
        self.subtitle_checkbox.clicked.connect(self.subtitle_checkbox_clicked)

        self.subtitle_template_edit = TemplateConfig(
            mi=self.test_book
        )  # device_settings.save_template)
        subtitle_template_edit_tooltip = _(
            "Enter a template to use to set the subtitle. If the template is empty, the subtitle will be cleared."
        )
        self.subtitle_template_edit.setToolTip(subtitle_template_edit_tooltip)
        options_layout.addWidget(self.subtitle_template_edit, widget_line, 2, 1, 2)

        widget_line += 1
        self.reading_direction_checkbox = QCheckBox(_("Reading direction"), self)
        reading_direction_checkbox_tooltip = _("Set the reading direction")
        self.reading_direction_checkbox.setToolTip(reading_direction_checkbox_tooltip)
        options_layout.addWidget(self.reading_direction_checkbox, widget_line, 0, 1, 1)
        self.reading_direction_checkbox.clicked.connect(
            self.reading_direction_checkbox_clicked
        )

        self.reading_direction_combo = ReadingDirectionChoiceComboBox(
            self, READING_DIRECTIONS
        )
        self.reading_direction_combo.setToolTip(reading_direction_checkbox_tooltip)
        options_layout.addWidget(self.reading_direction_combo, widget_line, 1, 1, 1)

        self.date_added_checkbox = QCheckBox(_("Date added"), self)
        date_added_checkbox_tooltip = _(
            "Set the date added to the device. This is used when sorting."
        )
        self.date_added_checkbox.setToolTip(date_added_checkbox_tooltip)
        options_layout.addWidget(self.date_added_checkbox, widget_line, 2, 1, 1)
        self.date_added_checkbox.clicked.connect(self.date_added_checkbox_clicked)

        self.date_added_column_combo = CustomColumnComboBox(self)
        self.date_added_column_combo.setToolTip(date_added_checkbox_tooltip)
        options_layout.addWidget(self.date_added_column_combo, widget_line, 3, 1, 1)

        widget_line += 1
        self.use_plugboard_checkbox = QCheckBox(_("Use plugboard"), self)
        self.use_plugboard_checkbox.setToolTip(
            _(
                "Set the metadata on the device using the plugboard for the device and book format."
            )
        )
        self.use_plugboard_checkbox.clicked.connect(self.use_plugboard_checkbox_clicked)
        options_layout.addWidget(self.use_plugboard_checkbox, widget_line, 0, 1, 2)

        self.update_kepubs_checkbox = QCheckBox(_("Update Kobo ePubs"), self)
        self.update_kepubs_checkbox.setToolTip(
            _("Update the metadata for kePubs downloaded from the Kobo server.")
        )
        options_layout.addWidget(self.update_kepubs_checkbox, widget_line, 2, 1, 2)

        self.readingStatusGroupBox = ReadingStatusGroupBox(
            cast("ui.Main", self.parent())
        )
        layout.addWidget(self.readingStatusGroupBox)

        layout.addStretch(1)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.ok_clicked)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def ok_clicked(self) -> None:
        new_prefs = cfg.MetadataOptionsConfig()
        new_prefs.title = self.title_checkbox.isChecked()
        new_prefs.titleSort = self.title_sort_checkbox.isChecked()
        new_prefs.author = self.author_checkbox.isChecked()
        new_prefs.authourSort = self.author_sort_checkbox.isChecked()
        new_prefs.description = self.description_checkbox.isChecked()
        new_prefs.descriptionUseTemplate = (
            self.description_use_template_checkbox.isChecked()
        )
        new_prefs.descriptionTemplate = self.description_template_edit.template
        new_prefs.publisher = self.publisher_checkbox.isChecked()
        new_prefs.published_date = self.published_checkbox.isChecked()
        new_prefs.isbn = self.isbn_checkbox.isChecked()
        new_prefs.rating = self.rating_checkbox.isChecked()
        new_prefs.series = self.series_checkbox.isChecked()
        new_prefs.usePlugboard = self.use_plugboard_checkbox.isChecked()
        new_prefs.language = self.language_checkbox.isChecked()
        new_prefs.update_KoboEpubs = self.update_kepubs_checkbox.isChecked()
        new_prefs.subtitle = self.subtitle_checkbox.isChecked()
        new_prefs.subtitleTemplate = self.subtitle_template_edit.template
        new_prefs.set_reading_direction = self.reading_direction_checkbox.isChecked()
        new_prefs.set_sync_date = self.date_added_checkbox.isChecked()

        if (
            new_prefs.descriptionUseTemplate
            and not self.description_template_edit.validate()
        ):
            return

        if new_prefs.subtitle and not self.subtitle_template_edit.validate():
            return

        if new_prefs.set_reading_direction:
            new_prefs.reading_direction = READING_DIRECTIONS[
                str(self.reading_direction_combo.currentText()).strip()
            ]

        if new_prefs.set_sync_date:
            new_prefs.sync_date_library_date = (
                self.date_added_column_combo.get_selected_column()
            )

        new_prefs.setRreadingStatus = (
            self.readingStatusGroupBox.readingStatusIsChecked()
        )
        if self.readingStatusGroupBox.readingStatusIsChecked():
            new_prefs.readingStatus = self.readingStatusGroupBox.readingStatus()
            if new_prefs.readingStatus < 0:
                error_dialog(
                    self,
                    "No reading status option selected",
                    "If you are changing the reading status, you must select an option to continue",
                    show=True,
                    show_copy_button=False,
                )
                return
            new_prefs.resetPosition = (
                self.readingStatusGroupBox.reset_position_checkbox.isChecked()
            )

        # Only if the user has checked at least one option will we continue
        for key, val in new_prefs:
            debug("key='%s' new_prefs[key]=%s" % (key, val))
            if val and key != "readingStatus" and key != "usePlugboard":
                cfg.plugin_prefs.MetadataOptions = new_prefs
                self.accept()
                return
        error_dialog(
            self,
            _("No options selected"),
            _("You must select at least one option to continue."),
            show=True,
            show_copy_button=False,
        )

    def update_title_sort_checkbox(self):
        self.title_sort_checkbox.setEnabled(
            self.title_checkbox.isChecked()
            and not self.use_plugboard_checkbox.isChecked()
        )
        if self.title_sort_checkbox.isEnabled():
            self.title_sort_checkbox.setToolTip(None)
        else:
            self.title_sort_checkbox.setToolTip("Not used when plugboard is enabled")

    def update_author_sort_checkbox(self):
        self.author_sort_checkbox.setEnabled(
            self.author_checkbox.isChecked()
            and not self.use_plugboard_checkbox.isChecked()
        )
        if self.author_sort_checkbox.isEnabled():
            self.author_sort_checkbox.setToolTip(None)
        else:
            self.author_sort_checkbox.setToolTip("Not used when plugboard is enabled")

    def description_checkbox_clicked(self, checked: bool):
        self.description_use_template_checkbox.setEnabled(checked)
        self.description_use_template_checkbox_clicked(checked)

    def description_use_template_checkbox_clicked(self, checked: bool):
        self.description_template_edit.setEnabled(
            checked and self.description_use_template_checkbox.isChecked()
        )

    def subtitle_checkbox_clicked(self, checked: bool):
        self.subtitle_template_edit.setEnabled(checked)

    def date_added_checkbox_clicked(self, checked: bool):
        self.date_added_column_combo.setEnabled(checked)

    def reading_direction_checkbox_clicked(self, checked: bool):
        self.reading_direction_combo.setEnabled(checked)

    def use_plugboard_checkbox_clicked(self):
        self.update_title_sort_checkbox()
        self.update_author_sort_checkbox()

    def get_date_columns(
        self, column_names: list[str] = DATE_COLUMNS
    ) -> dict[str, str]:
        available_columns: dict[str, str] = {}
        for column_name in column_names:
            calibre_column_name = self.gui.library_view.model().orig_headers[
                column_name
            ]
            available_columns[column_name] = calibre_column_name
        available_columns.update(self.get_date_custom_columns())
        return available_columns

    def get_date_custom_columns(self):
        column_types = ["datetime"]
        return self.get_custom_columns(column_types)

    def get_custom_columns(self, column_types: list[str]) -> dict[str, str]:
        custom_columns = self.gui.library_view.model().custom_columns
        available_columns = {}
        for key, column in custom_columns.items():
            typ = column["datatype"]
            if typ in column_types:
                available_columns[key] = column["name"]
        return available_columns


class TemplateConfig(QWidget):  # {{{
    def __init__(self, val: str | None = None, mi: Book | None = None):
        QWidget.__init__(self)
        self.mi = mi
        debug("mi=", self.mi)
        self.t = t = QLineEdit(self)
        t.setText(val or "")
        t.setCursorPosition(0)
        self.setMinimumWidth(300)
        self.l = layout = QGridLayout(self)
        self.setLayout(layout)
        layout.addWidget(t, 1, 0, 1, 1)
        b = self.b = QPushButton(_("&Template editor"))
        layout.addWidget(b, 1, 1, 1, 1)
        b.clicked.connect(self.edit_template)

    @property
    def template(self):
        return str(self.t.text()).strip()

    @template.setter
    def template(self, template: str):
        self.t.setText(template)

    def edit_template(self):
        t = TemplateDialog(self, self.template, mi=self.mi)
        t.setWindowTitle(_("Edit template"))
        if t.exec():
            self.t.setText(t.rule[1])

    def validate(self):
        from calibre.utils.formatter import validation_formatter

        tmpl = self.template
        try:
            validation_formatter.validate(tmpl)
            return True
        except Exception as err:
            error_dialog(
                self,
                _("Invalid template"),
                "<p>" + _("The template %s is invalid:") % tmpl + "<br>" + str(err),
                show=True,
            )

            return False


class ReadingDirectionChoiceComboBox(QComboBox):
    def __init__(
        self,
        parent: QWidget,
        reading_direction_list: dict[str, str] = READING_DIRECTIONS,
    ):
        QComboBox.__init__(self, parent)
        for name, font in sorted(reading_direction_list.items()):
            self.addItem(name, font)

    def select_text(self, selected_text: str):
        idx = self.findData(selected_text)
        if idx != -1:
            self.setCurrentIndex(idx)
        else:
            self.setCurrentIndex(0)
