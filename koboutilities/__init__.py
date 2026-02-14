# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import annotations

__license__ = "GPL v3"
__copyright__ = "2013-2020, David Forrester <davidfor@internode.on.net>"
__docformat__ = "restructuredtext en"
__version__ = (2, 26, 1)

# The class that all Interface Action plugin wrappers must inherit from
from typing import TYPE_CHECKING

from calibre.customize import InterfaceActionBase

if TYPE_CHECKING:
    from .config import ConfigWidget


class ActionKoboUtilities(InterfaceActionBase):
    """
    This class is a simple wrapper that provides information about the actual
    plugin class. The actual interface plugin class is called InterfacePlugin
    and is defined in the ui.py file, as specified in the actual_plugin field
    below.

    The reason for having two classes is that it allows the command line
    calibre utilities to run without needing to load the GUI libraries.
    """

    name = "Kobo Utilities"
    description = "Utilities to use with Kobo ereaders"
    supported_platforms = ["windows", "osx", "linux"]  # noqa: RUF012
    author = "David Forrester, Jan Larres, and others"
    version = __version__
    # Calibre versions from https://github.com/kovidgoyal/calibre/blob/master/bypy/sources.json:
    # Calibre 5.13.0 (2021-03-10): Python 3.8.5
    # Calibre 6.0.0 (2022-07-11): Python 3.10.1
    # Calibre 7.0.0 (2023-11-17): Python 3.11.5
    # Calibre 8.0.0 (2025-03-21): Python 3.11.5
    # Calibre 9.0.0 (2026-01-30): Python 3.14.1
    # Maintenance note: if you update the minimum version here,
    # make sure to also update it in scripts/run and .github/workflows/main.yml
    minimum_calibre_version = (5, 13, 0)

    #: This field defines the GUI plugin class that contains all the code
    #: that actually does something. Its format is module_path:class_name
    #: The specified class must be defined in the specified module.
    actual_plugin = "calibre_plugins.koboutilities.action:KoboUtilitiesAction"

    def is_customizable(self):
        """
        This method must return True to enable customization via
        Preferences->Plugins
        """
        return True

    def config_widget(self):
        """
        Implement this method and :meth:`save_settings` in your plugin to
        use a custom configuration dialog.

        This method, if implemented, must return a QWidget. The widget can have
        an optional method validate() that takes no arguments and is called
        immediately after the user clicks OK. Changes are applied if and only
        if the method returns True.

        If for some reason you cannot perform the configuration at this time,
        return a tuple of two strings (message, details), these will be
        displayed as a warning dialog to the user and the process will be
        aborted.

        The base class implementation of this method raises NotImplementedError
        so by default no user configuration is possible.
        """
        if self.actual_plugin_:
            from .config import ConfigWidget

            return ConfigWidget(self.actual_plugin_)
        return None

    def save_settings(self, config_widget: ConfigWidget):
        """
        Save the settings specified by the user with config_widget.

        :param config_widget: The widget returned by :meth:`config_widget`.
        """
        config_widget.save_settings()
