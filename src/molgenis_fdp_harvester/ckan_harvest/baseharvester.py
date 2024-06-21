# SPDX-FileCopyrightText: Open Knowledge
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/harvesters/base.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

import logging
import re

from molgenis_fdp_harvester.ckan_harvest.baseparser import (
    _munge_to_length,
    munge_tag,
    substitute_ascii_equivalents,
)


log = logging.getLogger(__name__)

PACKAGE_NAME_MIN_LENGTH = 2
PACKAGE_NAME_MAX_LENGTH = 100


def munge_title_to_name(name: str) -> str:
    """Munge a package title into a package name."""
    name = substitute_ascii_equivalents(name)
    # convert spaces and separators
    name = re.sub("[ .:/]", "-", name)
    # take out not-allowed characters
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    # remove doubles
    name = re.sub("-+", "-", name)
    # remove leading or trailing hyphens
    name = name.strip("-")
    # if longer than max_length, keep last word if a year
    max_length = PACKAGE_NAME_MAX_LENGTH - 5
    # (make length less than max, in case we need a few for '_' chars
    # to de-clash names.)
    if len(name) > max_length:
        year_match = re.match(r".*?[_-]((?:\d{2,4}[-/])?\d{2,4})$", name)
        if year_match:
            year = year_match.groups()[0]
            name = "%s-%s" % (name[: (max_length - len(year) - 1)], year)
        else:
            name = name[:max_length]
    name = _munge_to_length(name, PACKAGE_NAME_MIN_LENGTH, PACKAGE_NAME_MAX_LENGTH)
    return name


class HarvesterBase(object):
    """
    Generic base class for harvesters, providing a number of useful functions.

    A harvester doesn't have to derive from this - it could just have:

        implements(IHarvester)
    """

    config = None
    _user_name = None
    _gather_errors = []
    _harvest_objects = []

    def _save_gather_error(self, *args):
        log.warning("Harvester gather error: %s", args)
        self._gather_errors.append(args)

    @classmethod
    def _gen_new_name(cls, title, existing_name=None, append_type=None):
        """
        Returns a 'name' for the dataset (URL friendly), based on the title.

        If the ideal name is already used, it will append a number to it to
        ensure it is unique.

        If generating a new name because the title of the dataset has changed,
        specify the existing name, in case the name doesn't need to change
        after all.

        :param existing_name: the current name of the dataset - only specify
                              this if the dataset exists
        :type existing_name: string
        :param append_type: the type of characters to add to make it unique -
                            either 'number-sequence' or 'random-hex'.
        :type append_type: string
        """

        # If append_type was given, use it. Otherwise, use the configured default.
        # If nothing was given and no defaults were set, use 'number-sequence'.
        if append_type:
            append_type_param = append_type
        else:
            append_type_param = "number-sequence"

        ideal_name = munge_title_to_name(title)
        ideal_name = re.sub("-+", "-", ideal_name)  # collapse multiple dashes
        return cls._ensure_name_is_unique(
            ideal_name, existing_name=existing_name, append_type=append_type_param
        )

    @staticmethod
    def _ensure_name_is_unique(
        ideal_name, existing_name=None, append_type="number-sequence"
    ):
        """
        Returns a dataset name based on the ideal_name, only it will be
        guaranteed to be different than all the other datasets, by adding a
        number on the end if necessary.

        If generating a new name because the title of the dataset has changed,
        specify the existing name, in case the name doesn't need to change
        after all.

        The maximum dataset name length is taken account of.

        :param ideal_name: the desired name for the dataset, if its not already
                           been taken (usually derived by munging the dataset
                           title)
        :type ideal_name: string
        :param existing_name: the current name of the dataset - only specify
                              this if the dataset exists
        :type existing_name: string
        :param append_type: the type of characters to add to make it unique -
                            either 'number-sequence' or 'random-hex'.
        :type append_type: string
        """

        ideal_name = ideal_name[:PACKAGE_NAME_MAX_LENGTH]

    def _get_user_name(self):
        """
        Returns the name of the user that will perform the harvesting actions
        (deleting, updating and creating datasets)

        By default this will be the old 'harvest' user to maintain
        compatibility. If not present, the internal site admin user will be
        used. This is the recommended setting, but if necessary it can be
        overridden with the `ckanext.harvest.user_name` config option:

           ckanext.harvest.user_name = harvest

        """
        log.warning("_get_user_name: stubbed")
        return None

    def _create_harvest_objects(self, remote_ids, harvest_job):
        """
        Given a list of remote ids and a Harvest Job, create as many Harvest Objects and
        return a list of their ids to be passed to the fetch stage.

        TODO: Not sure it is worth keeping this function
        """
        log.warning("_create_harvest_objects: stubbed")

    def _create_or_update_package(
        self, package_dict, harvest_object, package_dict_form="rest"
    ):
        """
        Creates a new package or updates an existing one according to the
        package dictionary provided.

        The package dictionary can be in one of two forms:

        1. 'rest' - as seen on the RESTful API:

                http://datahub.io/api/rest/dataset/1996_population_census_data_canada

           This is the legacy form. It is the default to provide backward
           compatibility.

           * 'extras' is a dict e.g. {'theme': 'health', 'sub-theme': 'cancer'}
           * 'tags' is a list of strings e.g. ['large-river', 'flood']

        2. 'package_show' form, as provided by the Action API (CKAN v2.0+):

               http://datahub.io/api/action/package_show?id=1996_population_census_data_canada

           * 'extras' is a list of dicts
                e.g. [{'key': 'theme', 'value': 'health'},
                        {'key': 'sub-theme', 'value': 'cancer'}]
           * 'tags' is a list of dicts
                e.g. [{'name': 'large-river'}, {'name': 'flood'}]

        Note that the package_dict must contain an id, which will be used to
        check if the package needs to be created or updated (use the remote
        dataset id).

        If the remote server provides the modification date of the remote
        package, add it to package_dict['metadata_modified'].

        :returns: The same as what import_stage should return. i.e. True if the
                  create or update occurred ok, 'unchanged' if it didn't need
                  updating or False if there were errors.


        TODO: Not sure it is worth keeping this function. If useful it should
        use the output of package_show logic function (maybe keeping support
        for rest api based dicts
        """
        raise NotImplementedError("_create_or_update_package")

    def _find_existing_package(self, package_dict):
        raise NotImplementedError()

    def _clean_tags(self, tags):
        try:

            def _update_tag(tag_dict, key, newvalue):
                # update the dict and return it
                tag_dict[key] = newvalue
                return tag_dict

            # assume it's in the package_show form
            tags = [
                _update_tag(t, "name", munge_tag(t["name"]))
                for t in tags
                if munge_tag(t["name"]) != ""
            ]

        except TypeError:  # a TypeError is raised if `t` above is a string
            # REST format: 'tags' is a list of strings
            tags = [munge_tag(t) for t in tags if munge_tag(t) != ""]
            tags = list(set(tags))
            return tags

        return tags

    @classmethod
    def last_error_free_job(cls, harvest_job):
        raise NotImplementedError()
