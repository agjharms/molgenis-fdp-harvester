# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/harvesters/rdf.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

from builtins import str

# from past.builtins import basestring
import json
from typing import List
import logging
import hashlib
import traceback

from molgenis_fdp_harvester.ckan_harvest.baseharvester import munge_title_to_name
from molgenis.client import Session


# import ckan.plugins as p
# import ckan.model as model

# import ckan.lib.plugins as lib_plugins

# from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
# from ckanext.harvest.logic.schema import unicode_safe

# from ckanext.dcat.harvesters.base import DCATHarvester
from .dcatharvester import DCATHarvester

# from ckanext.dcat.processors import RDFParserException, RDFParser
from .processor import RDFParser, HarvesterException

# from ckanext.dcat.interfaces import IDCATRDFHarvester

log = logging.getLogger(__name__)


class HarvestObject(object):
    def __init__(self, guid, content):
        self.guid = guid
        self.content = content
        self.status = None


class DCATRDFHarvester(DCATHarvester):
    _names_taken = []

    def __init__(self, profiles: List, entity_name: str):
        super().__init__()
        self._profiles = profiles
        self.entity_name = entity_name

    def info(self):
        return {
            "name": "dcat_rdf",
            "title": "Generic DCAT RDF Harvester",
            "description": "Harvester for DCAT datasets from an RDF graph",
        }

    def _get_dict_value(self, _dict, key, default=None):
        """
        Returns the value for the given key on a CKAN dict

        By default a key on the root level is checked. If not found, extras
        are checked, both with the key provided and with `dcat_` prepended to
        support legacy fields.

        If not found, returns the default value, which defaults to None
        """

        if key in _dict:
            return _dict[key]

        for extra in _dict.get("extras", []):
            if extra["key"] == key or extra["key"] == "dcat_" + key:
                return extra["value"]

        return default

    def _get_guid(self, dataset_dict, source_url=None):
        """
        Try to get a unique identifier for a harvested dataset

        It will be the first found of:
         * URI (rdf:about)
         * dcat:identifier
         * Source URL + Dataset name
         * Dataset name

         The last two are obviously not optimal, as depend on title, which
         might change.

         Returns None if no guid could be decided.
        """
        guid = None

        guid = self._get_dict_value(dataset_dict, "uri") or self._get_dict_value(
            dataset_dict, "identifier"
        )
        if guid:
            return guid

        if dataset_dict.get("name"):
            guid = dataset_dict["name"]
            if source_url:
                guid = source_url.rstrip("/") + "/" + guid
        return guid

    def _mark_datasets_for_deletion(self, guids_in_source, harvest_job):
        """
        Given a list of guids in the remote source, checks which in the DB
        need to be deleted

        To do so it queries all guids in the DB for this source and calculates
        the difference.

        For each of these creates a HarvestObject with the dataset id, marked
        for deletion.

        Returns a list with the ids of the Harvest Objects to delete.
        """

        log.warning("_mark_datasets_for_deletion: stub")
        return None

    def validate_config(self, source_config):
        log.warning("validate_config: stub")

    def gather_stage(self, harvest_root_uri):

        log.debug("In DCATRDFHarvester gather_stage")

        rdf_format = None

        # Get file contents of first page
        next_page_url = harvest_root_uri

        guids_in_source = []
        last_content_hash = None
        self._names_taken = []

        parser = RDFParser(self._profiles)

        while next_page_url:
            if not next_page_url:
                # return []
                break

            content, rdf_format = self._get_content_and_type(
                next_page_url, 1, content_type=rdf_format
            )

            # MD5 is not cryptographically secure anymore, but this is not a security function.
            # It is used as a fast hash function to make sure no duplicate data is received
            content_hash = hashlib.md5()
            if content:
                content_hash.update(content.encode("utf8"))

            if last_content_hash:
                if content_hash.digest() == last_content_hash.digest():
                    log.warning(
                        "Remote content was the same even when using a paginated URL, skipping"
                    )
                    break
            else:
                last_content_hash = content_hash

            if not content:
                break
                # return []

            try:
                parser.parse(content, _format=rdf_format)
            except HarvesterException as e:
                self._save_gather_error(
                    "Error parsing the RDF file: {0}".format(e), next_page_url
                )
                # return []
                break

            if not parser:
                return []

            try:
                # Data
                for dataset in parser.dataset_in_catalog():
                    # get content
                    dataset_content, dataset_rdf_format = self._get_content_and_type(
                        dataset, 1, content_type=None
                    )
                    parser.parse(dataset_content, _format=dataset_rdf_format)
            except HarvesterException as e:
                self._save_gather_error(
                    "Error parsing the acquired dataset: {0}".format(e),
                )
                # return []
                break

            # get the next page
            # FIXME: separate this out now that parser is global (else it'll always return a next page that isn't necessarily THE next page)
            next_page_url = parser.next_page()

        try:
            # source_dataset = model.Package.get(harvest_job.source.id)
            for dataset in parser.datasets():
                if not dataset.get("name"):
                    dataset["name"] = self._gen_new_name(dataset["title"])
                if dataset["name"] in self._names_taken:
                    suffix = (
                        len(
                            [
                                i
                                for i in self._names_taken
                                if i.startswith(dataset["name"] + "-")
                            ]
                        )
                        + 1
                    )
                    dataset["name"] = "{}-{}".format(dataset["name"], suffix)
                self._names_taken.append(dataset["name"])

                # Unless already set by the parser, get the owner organization (if any)
                # from the harvest source dataset
                # if not dataset.get("owner_org"):
                #     if source_dataset.owner_org:
                #         dataset["owner_org"] = source_dataset.owner_org

                # Try to get a unique identifier for the harvested dataset
                guid = self._get_guid(dataset, source_url=dataset["uri"])

                # FIXME molgenis ID cannot be URI but has to be alphanumeric string
                dataset["id"] = munge_title_to_name(guid)
                # dataset["extras"].append({"key": "guid", "value": guid})

                if not guid:
                    self._save_gather_error(
                        "Could not get a unique identifier for dataset: {0}".format(
                            dataset
                        ),
                        # harvest_job,
                    )
                    continue

                guids_in_source.append(guid)

                obj = HarvestObject(guid=guid, content=json.dumps(dataset))

                self._harvest_objects.append(obj)
        except Exception as e:
            self._save_gather_error(
                "Error when processsing dataset: %r / %s" % (e, traceback.format_exc()),
            )
            return []

        # # Check if some datasets need to be deleted
        # object_ids_to_delete = self._mark_datasets_for_deletion(
        #     guids_in_source,
        # )

        # object_ids.extend(object_ids_to_delete)

        return self._harvest_objects

    def fetch_stage(self, molgenis_session: Session) -> List[str]:
        # Reusing the fetch stage to get a list of IDs
        # Note: very specific to current EUCAIM collections
        try:
            self._existing_dataset_guid = [
                x["id"] for x in molgenis_session.get(self.entity_name)
            ]
        except Exception as e:
            log.error(
                "fetch_stage: Error getting list of uids %s: %r / %s",
                (self.entity_name, e, traceback.format_exc()),
            )
            self._existing_dataset_guid = []

        return self._existing_dataset_guid

    def import_stage(self, harvest_object: HarvestObject, molgenis_session: Session):

        log.debug("In DCATRDFHarvester import_stage")

        status = self._get_object_extra(harvest_object, "status")
        if status == "delete":
            log.warning("import_stage: deleting datasets is currently not supported")
            return True

        if harvest_object.content is None:
            log.error(
                "import_stage: Empty content for object {0}".format(harvest_object.id),
            )
            return False

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            log.error(
                "import_stage: Could not parse content for object {0}".format(
                    harvest_object.id
                ),
            )
            return False

        dataset = self.modify_package_dict(dataset, {}, harvest_object)

        # Check if a dataset with the same guid exists
        try:
            if harvest_object.guid in self._existing_dataset_guid:
                log.info("Updating dataset %s" % dataset["name"])
                molgenis_session.update_all("EUCAIM_collections", [dataset])
            else:
                log.info("Adding dataset %s" % dataset["name"])
                molgenis_session.add(self.entity_name, dataset)
        except Exception as e:
            log.error(
                "import_stage: Error importing dataset %s: %r / %s"
                % (dataset.get("name", ""), e, traceback.format_exc()),
            )
            return False

        return True
