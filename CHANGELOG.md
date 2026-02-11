# Changelog

## Version 2.26.0 - 2026-02-11

- Added feature to restore a previously created backup to a device.
- Always include the database in the backup ZIP for simplicity, space savings,
  and easier restore.
- Set a default backup location so that forgetting to specify a directory
  doesn't cause the backup feature to silently not do anything.
- Updated fonts in the "Set reader font" feature to firmware 4.39.22801.
- Fixed some translations not loading correctly in some cases.
- Clarified annotations menu entries a bit.

## Version 2.25.0 - 2026-01-11

- Moved the documentation to a GitHub wiki for easier access and maintenance.
- Fixed a few bugs where some books where incorrectly marked as needing update
  when comparing tables of contents.
- Fixed an exception when opening the Related Books feature on supported
  firmware versions.
- Fixed a small incompatibility with Calibre 9.

## Version 2.24.5 - 2025-10-06

- Fixed an error that prevented the "Remove annotation files" feature from working

## Version 2.24.4 - 2025-10-05

- Fixed the Table of Contents update sometimes failing with a
  "UNIQUE constraint failed" error and messing up the ToC on the device.
  This also fixes an issue in the same dialog where it sometimes said that
  the ToC needs to be updated even though it was up to date.
- Support showing the changelog in the About dialog

## Version 2.24.3 - 2025-09-09

- Fixed the config dialog not saving individual device backup options correctly

## Version 2.24.2 - 2025-08-17

- Fixed profiles losing their device associations after updating the devices
  to use serial numbers
- Make sure to compress the backup ZIP file
- Added changelog link to About dialog

## Version 2.24.1 - 2025-08-11

- Fixed error when trying to automatically store locations after switching libraries

## Version 2.24.0 - 2025-08-10

- Devices are now identified by their serial number instead of the
  Calibre-assigned UUID. This means that they will be recognized even after
  a reset. Existing configurations will be updated.
- Fixed multiple issues where settings weren't being saved properly
- Fixed issue where the configured collection of the "Get collections from
  device" feature was saved globally instead of per library
- Fixed an issue where sometimes the wrong profile got associated with a device
- Replaced some icons in the config dialog with more intuitive ones
- Show the associated profile in the device list
- Don't change the name returned by the `connected_device_name()` template
  function when renaming devices
- Allow renaming non-connected devices
- Allow renaming a device by double-clicking the name in the device list
- Added tooltips in "Update metadata" dialog for disabled options

## Version 2.23.0 - 2025-05-05

- Remove obsolete "Order series collections" feature
- Re-enable "Manage series information" feature
- Show Help link in plugin menu
- Mention automatic metadata management in documentation
- Add some menu icons

## Version 2.22.1 - 2025-04-20

- Fixed the metadata update dialog not finding the correct plugboards

## Version 2.22.0 - 2025-04-14

- Disabled "Manage series information" and "Order series collections" features
  as they are obsolete. They are set to be removed in a future release.
- Changed all instances of 'shelves' to 'collections' for consistency
  with current firmware versions (partly by @chocolatechipcats)
- Changed all instances of 'current bookmark' to 'reading position'
  for clarity and consistency
- Fixed some errors when using old firmware versions
- Documented that newer Tolino devices are supported as well
- Added `scripts/run` task to update translation template
- Added section about contributing translations to README
- Automatically compile translations when building plugin
- Made text case more consistent
- Fixed some language and translation issues

## Version 2.21.1 - 2025-03-26

- Fixed a bug that prevented the plugin from working
  in supported Calibre versions before 5.41

## Version 2.21.0 - 2025-03-24

- Always show all menu items to improve discoverability and reduce confusion.
  If a feature is not currently available the menu item will be disabled
  and a tooltip will be shown explaining why.
- Fixed another bug in the "Remove Annotation Files" feature
- Correctly update the "Device" tab in the options when connecting a device
  while the dialog is open

## Version 2.20.0 - 2025-03-22

- Use Calibre 8's database handling if available
  to prevent potential data loss on some filesystems
- Don't show driver switching options in Calibre 8 since it is no longer necessary
- Fixed bug that prevented some options from showing up in the Upload Covers dialog
- Fixed long-standing bug that prevented the "Remove Annotation Files"
  feature from working
- Made debug logging more consistent

## Version 2.19.1 - 2025-03-17

- Fixed error when using epub files instead of kepub

## Version 2.19.0 - 2025-03-16

- Removed support for modifying home tiles
  as it is not supported in current firmware versions
- Fixed an issue that prevented device rename and delete buttons from being
  enabled when only one device was registered
- Fixed a potential exception when ejecting devices

## Version 2.18.4 - 27 Feb 2025

- Fix "attribute not found" error when displaying reading position changes dialog

## Version 2.18.3 - 27 Feb 2025

- Fix error when updating metadata for a book that doesn't have a published date

## Version 2.18.2 - 24 Feb 2025

- Fix error when displaying "Show Reading Position Changes" dialog
  if a book doesn't have a last-read date set

## Version 2.18.1 - 12 Feb 2025

- Fix error when compressing the device database

## Version 2.18.0 - 8 Feb 2025

- Add two new syncable columns: "Time Spent Reading" and "Rest of Book Estimate"
- Fix bug that prevented books from being deselected in reading position dialog
- A few minor fixes
- Various internal cleanups that raise the minimum supported Calibre version to 5.13.0
- Update maintainer information

## Version 2.17.2 - 18 Oct 2024 (changes by @ownedbycats, release by @chaley)

- Minor tooltip correction

## Version 2.17.1 - 12 July 2024 (changes and release by @chaley)

- Remove the non-working firmware update check

## Version 2.16.13 - 11 Dec 2023 (changes and release by @chaley)

- Fix rare error where the Kobo database contains invalid dates or times

## Version 2.16.12 - 06 Dec 2023 (changes by @ownedbycats, release by @chaley)

- Improvement of help string for storing bookmarks

## Version 2.16.11 - 08 Nov 2023 (changes by @ownedbycats, release by @chaley)

- Support for long-text/comment-type columns for storing reading position.

## Version 2.16.10 - 12 July 2023 (changes & release by chaley)

- Fix help file not displaying on Windows.

## Version 2.16.9 - 4 July 2023 (changes by @ownedbycats, release by @chaley)

- Added Rakuten Sans/Serif support for the fonts dialog

## Version 2.16.8 - 4 July 2023 (changes by @Terisa de morgan, release by @chaley)

- Fix: Error when checking for firmware updates.

## Version 2.16.7 - 11 May 2023 (by @chaley)

- Fix: Problem when updating the ToC.

## Version 2.16.6 - 01 August 2022

- Fix: Qt6 compatiblility - Connecting actions to Radio buttons works differently.

## Version 2.16.5 - 20 July 2022

- Fix: Qt6 compatiblility - Prefs viewer tab stops and file chooser for database backup.

## Version 2.16.4 - 13 July 2022

- Fix: Qt6 compatiblility - Error if copies to keep option in configuration is not set.

## Version 2.16.3 - 28 May 2022

- Fix: Error when opening configuration and device specific options were being used.

## Version 2.16.2 - 25 May 2022

- Change: Allow device to be renamed when it is not connected.
- Fix: Error when using custom date column when setting metadata in library.

## Version 2.16.0 - 10 May 2022

- Fix: Fix removing the rating when rating is set.
- Change: Add option to not set font if already set on the device.

## Version 2.15.4 - 09 April 2022

- New: Add BookReader.sqlite to backup

## Version 2.15.3 - 01 March 2022

- Change: Show device name in button tooltip.
- Change: Better handling of device name and serial number.

## Version 2.15.2 - 07 January 2022

- Change: Calibre v6/Qt6 migration - Code cleanup

## Version 2.15.1 - 06 January 2022

- Change: Calibre v6/Qt6 migration - Remove use of QTableWidgetItem.UserType in common_utils.py.
- Fix: In some places the text for the rating and last read code was swapped.
- Change: Handle cancelling the custom column creation better.

## Version 2.15.0 - 04 January 2022

- New: Use CreateNewCustomColumn to create custom columns in the configuration dialog.
- Change: Update importing of some Qt classes as extremely early calibre v6/Qt6 migration.

## Version 2.14.4 - 28 December 2021

- Change: Some code and comments cleanup
- Fix: If there were multiple copies of a book on the device, setting and removing fonts, only did one.
- Fix: ToC rebuild for kepubs failed if the contentID had a dash followed by numbers in it. Should have anchored the regex to the end of the line.

## Version 2.14.1 - 18 July 2021

- Fix: "DateModified" not qualified in query used in Order Shelves.

## Version 2.14.0 - 01 June 2021

- New: ToC updater.

## Version 2.13.1 - 28 March 2021

- Fix: Error fetching reading status when there are multiple copies of the book on the device and the first has not been opened.

## Version 2.13.0 - 19 March 2021

- Change: Change how books are queued when automatically fetching reading locations.
- Change: Sort the results when getting the reading locations.
- New: Choose colour used for letterboxing in covers.
- Fix: Fix handling of epub locations for recent firmware.
- Fix: Error in Manage series on device.

## Version 2.12.3 - 12 January 2021

- Fix: Better handling when configuration of custom columns doesn't match existing columns.
- Fix: Validate selected profile when restoring/fetching reading status.
- Change: Only display tile related and the set related menu items for firmware before 4.4.0
- Change: Only show menu items that will might do something in current view.

## Version 2.12.0 - 05 January 2021

- Fix: Not updating series info properly if the series number in the database is null.
- Fix: Error in Manage Series on device if the book selected had a series, but no series index.
- Fix: Add some more logging in reading status fetch.
- Fix: Handle when percent read column doesn't exist when storing the bookmark.
- Fix: Update code in store bookmark when not run in background.
- Fix: Python 3 error slipped through when updating the foreground store bookmark.
- Fix: Fix handling when location is null on device but not in library.
- New: Add function to set the time on the device.
- Fix: Disable "Clear if unread" if "Not if finished in library" is selected.
- Fix: Another change to the handling when book is finished.

## Version 2.11.8 - 10 October 2020

- Fix: Python 3 related problem with ordering shelves.

## Version 2.11.7 - 7 October 2020

- Fix: Problem fetching the status if the LastReadDate is null.
- Fix: Correct string handling in about and help options.

## Version 2.11.5 - 25 September 2020

- Fix: More fixes for Python 3.

## Version 2.11.4 - 17 July 2020

- Fix: Updating parsing of firmware update file name to get the version number.
- Change: Display lookup name for columns in Date added choice in Update metadata dialog.
- Change: Add custom date columns and file timestamp option to Date added choices in Update metadata dialog.
- Fix: More fixes for Python 3.
- Fix: Error if setting font in config file if no "Reading" section.

## Version 2.11.0 - 03 March 2020

- New: Set SeriesID and SeriesNumberFloat for Series Tab support in 4.20.x.

## Version 2.10.0 - 08 February 2020

- Update: Changes for Python 3 support in calibre.
- Update: Rework some query building.
- Fix: Wasn't handling case were device specific settings were being used, but, there were none.
- Fix: Disable "Get Shelves From Device" when in the device list.
- Fix: Problem in sorting when using "Order Series Date"

## Version 2.9.0 - 13 October 2019

- Update: Set "get_cover" to as appropriate when getting metadata. This should improve performance in some places.
- Update: Handle changed reading location for epub starting with 4.17.13651. Should be backwardly compatible with currently stored locations and older firmware.
- New: Add options to cover updating for dithering, letterboxing and PNGs to match the driver change. Based on work from @NiLuJe.
- New: Add option to remove the full sized cover image.

## Version 2.8.0 - 20 April 2019

- Fix: Missed a change in the annotation builder to handle recent change in BeautifulSoup in calibre.
- Fix: Improve layout of results when displaying annotations.
- Update: Add new font "AR UDJingxihei" to font settings dialog.

## Version 2.7.0 - 28 March 2019

- New: Option to open cover image directory.
- Fix: Set the `___SyncTime` when setting the LastDateRead if the `___SyncTime` is later.
- Fix: Change annotation builder to handle recent change in BeautifulSoup.

## Version 2.6.0 - 6 September 2017

- New: Add setting sync date from calibre added or modified dates, or published date.
- New: Add French translation of help. Thanks to Frenchdummy.
- Fix: Error opening configuration if no devices and backup is set to individual configuration.

## Version 2.5.2 - 10 January 2017

- Fix: Button wasn't opening driver configuration if device wasn't connected.

## Version 2.5.1 - 9 January 2017

- Fix: Reenable "About Plugin"
- New: French translation from Eric (Infernoweb) and Alain (FrenchDummy)

## Version 2.5.0 - 5 January 2017

- Fix: Error if cleaning cover for book on device but not in database.
- Change: Button can be set to opening driver configuration or swapping drivers when no device connected.
- Fix: Exception during backup as WinError is not on non-Windows machines.
- Update: Latest Spanish translations from Terisa

## Version 2.4.1 - 25 Aug 2016

- Fix: Error creating trigger to block analytics.

## Version 2.4.0 - 20 Aug 2016

- Fix: Error deleting trigger.
- Fix: No progress bar for series management.
- Change: Change database reading to use the apsw library instead of sqlite3 library. This is to match changes in the KoboTouch driver.
- Change: Better handling of the progress bar.
- Fix: Timestamp issues when updating metadata.

## Version 2.3.2 - 24 May 2016

- Fix: Error updating metadata if the comments was empty.

## Version 2.3.1 - 18 May 2016

- Fix: Error when setting description but not using a template.

## Version 2.3.0 - 16 May 2016

- Change: Use template for subtitle.
- Change: Use template editor for comments and subtitle templates

## Version 2.2.0 - 9 April 2016

- New: Option to show Goodreads Sync "Update reading progress" dialog
- Update: After storing book status, select them in the library view
- New: Metadata uppdate - Update comments using a "jacket" or plugboard style template.
- New: Metadata uppdate - Option to update downloaded kepubs as well sideloaded books.
- New: Metadata uppdate - Option to set or clear the subtitle on the device.
- Update: Display progress dialog when getting book list for updating metadata.
- New: Submenu for driver configuration
- New: Menu option to swap between main and extended driver.
- New: Menu option to open driver configuration. If a device is connected, will open its configuration. Otherwise, it opens whichever is enable of the main and extended drivers.
- New: Display device name, firmware version and the driver name in tooltip of button.

## Version 2.1.0 - 6 December 2015

- New: Add "Display Extras Tiles".
- Fix: Error in getting reading settings from device configuration.
- Change: Disable tiles related menu items if firmware doesn't support them.
- Change: Update font names used for firmware 3.19.x

## Version 2.0.7 - 26 April 2015

- Fix: Profile name wasn't being passed to the reading status update dialog
- Fix: Messed up logging in jobs

## Version 2.0.6 - 26 April 2015

- Fix: Handle missing files in the backup properly.
- Change: add select/clear all buttons to reading status update dialog

## Version 2.0.5 - 29 January 2015

- Fix: Fix another error in migrating the settings.

## Version 2.0.4 - 21 January 2015

- Fix: Wasn't correctly doing the daily backup if the database in the zip file option was selected.

## Version 2.0.3 - 20 January 2015

- Fix: Error if iOS device plugged in and iOS reader applications and Marvin plugins are active.
- Change: Updated way backup files were deleted to handle when setting for putting database in the zip file is changed.
- Change: Turn on debug logging for backup job.

## Version 2.0.2 - 4 January 2015

- Fix: More errors reading configuration after the migration
- Fix: Opening the configuration shortly after ejecting the device gave an error

## Version 2.0.1 - 4 January 2015

- Fix: Errors reading configuration after the migration

## Version 2.0.0 - 3 January 2015

- Release with fixes changes in 1.8.6-1.8.13

## Version 1.8.13 - 30 December 2014

- New: Migrate library preferences to a "Migrated" profile.

## Version 1.8.12 - 30 December 2014

- Fix: Setting value into the db_prefs_backup.json file.
- Fix: Removing image and annotations files that have square brackets as part of the name
- Fix: Debug statement comparing dates when doing non-job store.

## Version 1.8.11 - 28 December 2014

- New: Set related books for sideloaded books

## Version 1.8.10 - 24 December 2014

- New: Remove annotations files
- Change: Use function for database path everywhere

## Version 1.8.9 - 20 December 2014

- Fix: Error when using update metadata or reading status
- Update: Latest Spanish translations from Terisa
- New: Added backup option to put database in the zip file with the config files

## Version 1.8.8 - 16 December 2014

- New: Move backup and firmware check to devices tab
- New: Option configure backup and firmware check for all devices or each device
- New: Show version number for connected device in device list
- Fix: File name for backups was very wrong
- Update: Profile selection added to store/restore dialog

## Version 1.8.7 - 14 December 2014

- New: Add serial number to device list, fix getting profile if none

## Version 1.8.6 - 14 December 2014

- New: Handle multiple devices better

## Version 1.8.5 - 13 December 2014

- Fix: Latest duplicate shelves has all timestamps set to "1970-01-01T00:00:00Z". Need to use ids if all the dates are the same.
- New: Display progress bar when removing the duplicate shelves.

## Version 1.8.4 - 2 December 2014

- Fix: Correct the name of the option for the backup on connection

## Version 1.8.3 - 22 November 2014

- Fix: Correct the name of the option for the backup on connection

## Version 1.8.2 - 21 November 2014

- New: Add option to do backup each time the device is connected.
- Fix: Error building list of shelves when fetching from device.
- Fix: Update menu handling for calibre 2.10 and later.

## Version 1.8.1 - 9 November 2014

- Fix: Added trigger for UPDATE to the Activity table
- Fix: Change way debug logging is handled in jobs

## Version 1.8.0 - 5 October 2014

- New: Copy shelves from device to one column
- Change: Backup config, affiliate.conf and version files and ADE registration

## Version 1.7.3 - 19 August 2014

- Fix: Wasn't correctly removing old backup files.

## Version 1.7.2 - 29 July 2014

- Change: Qt5 changes

## Version 1.201.1 - 17 July 2014

- Fix: Error in metadata update when setting series and using plugboard

## Version 1.201.0 - 13 July 2014

- Fix: Qt5 changes

## Version 1.7.1 - 5 July 2014

- Fix: Error in metadata update when setting series and using plugboard

## Version 1.7.0 - 16 June 2014

- Change: Changed the series shelf ordering to allow ordering of other shelves and different sorting.

## Version 1.6.11 - 6 June 2014

- Fix: Changed query used to fetch shelves for series ordering to improve performance.

## Version 1.6.9 - 14 May 2014

- Fix: Commented out job logging as it was causing problems.

## Version 1.6.8 - 12 May 2014

- Fix: Fix the problem with Series management and metadata updating properly.

## Version 1.6.7 - 12 May 2014

- Fix: Series management from device list was not updated for new options in metadata updating.
- Change: Background job logging as seem to be blowing the size of a buffer.

## Version 1.6.6 - 10 May 2014

- Change: When storing book status, use the status from the furtherest along copy if there are multiple copies. This is latest status, date or percent read.
- Change: Make jobs DeviceJobs so they won't get run at same time as initial device jobs.

## Version 1.6.5 - 22 April 2014

- Fix: Debug statement had reference to "newmi" when it should have been "book"

## Version 1.6.4 - 20 April 2014

- Fix: Hard coded number of values in line spacing spin button.
- Update: Line spacing changed with 3.2.0
- Update: If store has "Not finished in library" selected, only fetch status for books that haven't been finished.

## Version 1.6.3 - 12 April 2014

- Fix: Setting reading status from device list was not updated for new options in metadata updating.

## Version 1.6.2 - 28 March 2014

- Fix: Older devices had the MAC address in the serial number field of the version file. Strip the colons from this so it can be used in the backup file name.

## Version 1.6.1 - 22 March 2014

- Fix: Left a debug line in for backup file name.

## Version 1.6.0 - 22 March 2014

- New: Added tiles: "Release Note", CategoryFTE
- New: Firmware check and download
- New: Automatic backup of the device database
- New: After batch reading position store, display list of changes.
- Changed: Added options for using sort versions of title and author or the plugboard when updating metadata.
- New: Spanish translation.
- Fix: Clean images directory handles new images storage in FW2.9.0 and later

## Version 1.5.0 - 18 December 2013

- Release.

## Version 1.4.7 - 10 December 2013

- Fix: Fix handling of last read timestamp if it is null in the library or on the device

## Version 1.4.6 - 10 December 2013

- New: Added "Store if more recent" option for autostore
- New: If click button and now device connected, open configuration

## Version 1.4.5 - 09 December 2013

- Update: Change reading location store to not update library if no changes.
- New: Add auto store when device detected.
- New: Added progress bars when creating store jobs and updating library
- New: Added dismissing "In the cloud" tiles

## Version 1.4.4 - 30 November 2013

- Fix: Error displaying sizes after a database compression

## Version 1.4.3 - 22 November 2013

- Fix: Bad string handling in a debug statement.

## Version 1.4.2 - 20 November 2013

- New: Added internationalization
- Fix: Issue with format of timestamps in device database. Needed to add timezone info to some.

## Version 1.4.1 - 28 October 2013

- Update: Option added when ordering series shelf to update shelf sorting in config file

## Version 1.4.0 - 10 October 2013

- Released

## Version 1.3.2 - 10 October 2013

- New: Add "Lock margins" checkbox to reader settings to set the right margin the same as the left
- New: Add "Update config file" checkbox to reader settings to write the options to the "Kobo eReader.conf"

## Version 1.3.1 - 03 October 2013

- New: For shelves that match a series name, order the books by date added.

## Version 1.3.0 - 01 October 2013

- Update: Handle new set of line heights
- New: Fix Duplicate shelves

## Version 1.2.7 - 21 September 2013

- Fix: Finished fixing handling of older database versions with no ratings table.

## Version 1.2.6 - 20 September 2013

- Fix: Fix handling of older database versions with no ratings table.

## Version 1.2.5 - 07 September 2013

- Fix: Extra space after "false" in tile dismiss SQL

## Version 1.2.4 [beta] - 01 September 2013

- New: Support for Kobo WiFi
- Fix: Check for support of TIMESTAMP_STRING in device driver

## Version 1.2.3 - 05 August 2013

- New: Add function to create trigger to remove AnalyticEvents
- Fix: Spelling error in tooltip on Dismiss Tiles dialog
- New: Add dismissing of new and finished books
- Fix: Error when restoring reading location for kepubs
- New: Changes to handle new db engine

## Version 1.2.2 - 22 July 2013

- Fix: Error in image directory checking of no SD card

## Version 1.2.1 - 18 July 2013

- Fix: Wasn't getting reading state from device for books that had been marked as read but never opened.

## Version 1.2.0 - 16 July 2013

- Fix: Compress size display said "GB" instead of "MB".
- Fix: Had "Gil Sans" instead of "Gill Sans"

## Version 1.1.3 - 10 July 2013

- Fix: Error running store as background job.
- New: Added compress device database

## Version 1.1.2 - 03 July 2013

- New: Check covers directory for extra image files

## Version 1.1.1 - 21 June 2013

- New: Change store to background job

## Version 1.1.0 - 11 June 2013

- New: Create/delete database trigger for dismissing tiles
- Update: Support sideloaded kepubs for reading location
- Fix: If rating wasn't set, attempted to divide None by 2.
- Fix: Keep reference for all menu items to solve problem with OSX

## Version 1.0.0 - 09 June 2013

- Initial release

## Version 0.0.29 - 07 June 2013

- Fix: Name of shortcut for Store/Restore
- Update: Dismiss tiles menu option
- Update: More help written
- Update: Added anchors for all help. Will only be used on Linux
- Fix: When restoring reading position, could set percent read to null and DateLastRead format was not same as device uses.
- Fix: If last read date was empty in calibre, don't set it in the database on restore.

## Version 0.0.28 - 02 June 2013

- New: Removed "Mark as not interested". I have no real proof this works. Will investigate later.

## Version 0.0.27 - 31 May 2013

- New: Added dialog to dismiss tiles from new home screen

## Version 0.0.26 - 27 May 2013

- Fix: Annotations wasn't starting
- Fix: Upload covers if no books on device selected
- Fix: Upload covers didn't check if Kobo kepub was on SD card for skipping upload

## Version 0.0.25 - 07 May 2013

- Fix: Manage Series needed update to latest calibre code
- Fix: Fixed remove cover
- Fix: Upload covers sent covers to main memory for books on SD card

## Version 0.0.24 - 05 May 2013

- Fix: Debug error if no last read column

## Version 0.0.23 - 02 May 2013

- New: Run 'PRAGMA integrity_check' on the database

## Version 0.0.22 - 25 April 2013

- New: Disable rating option if no rating column
- Fix: Put rating option in first column of metadata options dialog
- New: Configure action for toolbar button
- New: Retrieve and restore the last read timestamp

## Version 0.0.21 - 23 April 2013

- New: Added language to metadata update
- New: Added configuration dialog

## Version 0.0.20 - 21 March 2013

- New: Mark recommendations as "Not Interested".
- Fix: Not in database dialog works properly.

## Version 0.0.19 - 11 March 2013

- Fix: Error when book has only just been sent to device and try to set metadata.
- New: Dialog to list books not in the device database

## Version 0.0.18 - 08 March 2013

- Fix: Set "FirstTimeReading" to true when restoring reading position or setting status

## Version 0.0.17 - 08 March 2013

- Added kepub option for cover removing

## Version 0.0.16

- Added "Keep aspect ratio" for cover uploading
- Added kepub option for cover uploading
- Fixed: ISBN option on dialog wasn't correct
- Removed contentID from stored reading position

## Version 0.0.12

- Added Current reading position dialog
- Added: Retrieve book reading settings from device

## Version 0.0.11 - 10 December 2012

- Added button to get reader settings for a single book from device database.
- Fixed setting of reader settings from options and config file.
- New: support for storing and restoring current reading location
- New: Backup database
- Fixed: Handling multiple copies of book on device.
- New: Backup annot file

## Version 0.0.10 - 5 December 2012 (2)

- Added option to refresh the books from the device
- After managing series, force a write of the metadata.calibre.
- Fix: Published date was being updated when it hadn't changed.

## Version 0.0.10 - 5 December 2012

- Fix: Error with missing ISBN option when managing series on device.
- Fix: Message when uploading covers.
- Toolbar button now does something on device list if view has something selected
- Handle non-numerics in series field of database.
- Added clean title of series info for Kobo books

## Version 0.0.9 - 3 December 2012

- Fix error in return from removing covers
- Added ISBN to metadata updating

## Version 0.0.8 - 2 December 2012

- Fix error in uploading cover
- Added finished messages to all actions when changes completed.

## Version 0.0.7 - 1 December 2012

- Added firmware version checking to the series support.
- Display selected line margin in custom lines spacing field
- Fixed date handling for pubdate.

## Version 0.0.6 - 30 November 2012

- Added custom entry for line spacing.
- Added published date to metadata update
- Added reset position when updating reading status

## Version 0.0.5 - 29 November 2012

- Maximum margin size changed to 16 for FW 2.3.0.

## Version 0.0.1 - 18 November 2012

- Initial creation of KoboUtilities plugin
