# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://github.com/ckan/ckanext-dcat/blob/master/ckanext/dcat/harvesters/rdf.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

from builtins import str

# from past.builtins import basestring
import json
import uuid
import logging
import hashlib
import traceback

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


class DCATRDFHarvester(DCATHarvester):

    def info(self):
        return {
            "name": "dcat_rdf",
            "title": "Generic DCAT RDF Harvester",
            "description": "Harvester for DCAT datasets from an RDF graph",
        }

    _names_taken = []

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

    def gather_stage(self, harvest_job, harvest_root_uri):
        log.debug("In DCATRDFHarvester gather_stage")

        rdf_format = None
        # if harvest_job.source.config:
        #     rdf_format = json.loads(harvest_job.source.config).get("rdf_format")

        # Get file contents of first page
        # next_page_url = harvest_job.source.url
        next_page_url = harvest_root_uri

        guids_in_source = []
        object_ids = []
        last_content_hash = None
        self._names_taken = []

        # TODO: profiles conf
        parser = RDFParser()

        while next_page_url:
            if not next_page_url:
                # return []
                break

            content, rdf_format = self._get_content_and_type(
                next_page_url, harvest_job, 1, content_type=rdf_format
            )

            # MD5 is not cryptographically secure anymore, but this is not a security concern.
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

            # TODO: store content?
            # Plugin isnt implemented in practice.
            # for harvester in p.PluginImplementations(IDCATRDFHarvester):
            #     content, after_download_errors = harvester.after_download(
            #         content, harvest_job
            #     )

            #     for error_msg in after_download_errors:
            #         self._save_gather_error(error_msg, harvest_job)

            if not content:
                break
                # return []

            try:
                parser.parse(content, _format=rdf_format)
            except HarvesterException as e:
                self._save_gather_error(
                    "Error parsing the RDF file: {0}".format(e), harvest_job
                )
                # return []
                break

            # for harvester in p.PluginImplementations(IDCATRDFHarvester):
            #     parser, after_parsing_errors = harvester.after_parsing(
            #         parser, harvest_job
            #     )

            #     for error_msg in after_parsing_errors:
            #         self._save_gather_error(error_msg, harvest_job)

            if not parser:
                return []

            try:
                # Data
                for dataset in parser.dataset_in_catalog():
                    parser.parse(dataset, _format=rdf_format)
            except HarvesterException as e:
                self._save_gather_error(
                    "Error parsing the acquired dataset: {0}".format(e), harvest_job
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
                if not dataset.get("owner_org"):
                    if source_dataset.owner_org:
                        dataset["owner_org"] = source_dataset.owner_org

                # Try to get a unique identifier for the harvested dataset
                guid = self._get_guid(dataset, source_url=source_dataset.url)

                if not guid:
                    self._save_gather_error(
                        "Could not get a unique identifier for dataset: {0}".format(
                            dataset
                        ),
                        harvest_job,
                    )
                    continue

                dataset["extras"].append({"key": "guid", "value": guid})
                guids_in_source.append(guid)

                obj = HarvestObject(
                    guid=guid, job=harvest_job, content=json.dumps(dataset)
                )

                obj.save()
                object_ids.append(obj.id)
        except Exception as e:
            self._save_gather_error(
                "Error when processsing dataset: %r / %s" % (e, traceback.format_exc()),
                harvest_job,
            )
            return []

        # Check if some datasets need to be deleted
        object_ids_to_delete = self._mark_datasets_for_deletion(
            guids_in_source, harvest_job
        )

        object_ids.extend(object_ids_to_delete)

        return object_ids

    def fetch_stage(self, harvest_object):
        # Nothing to do here
        return True

    def import_stage(self, harvest_object):

        log.debug("In DCATRDFHarvester import_stage")

        status = self._get_object_extra(harvest_object, "status")
        if status == "delete":
            # Delete package
            context = {
                "model": model,
                "session": model.Session,
                "user": self._get_user_name(),
                "ignore_auth": True,
            }

            try:
                p.toolkit.get_action("package_delete")(
                    context, {"id": harvest_object.package_id}
                )
                log.info(
                    "Deleted package {0} with guid {1}".format(
                        harvest_object.package_id, harvest_object.guid
                    )
                )
            except p.toolkit.ObjectNotFound:
                log.info(
                    "Package {0} already deleted.".format(harvest_object.package_id)
                )

            return True

        if harvest_object.content is None:
            self._save_object_error(
                "Empty content for object {0}".format(harvest_object.id),
                harvest_object,
                "Import",
            )
            return False

        try:
            dataset = json.loads(harvest_object.content)
        except ValueError:
            self._save_object_error(
                "Could not parse content for object {0}".format(harvest_object.id),
                harvest_object,
                "Import",
            )
            return False

        # Get the last harvested object (if any)
        previous_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current == True)
            .first()
        )

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        context = {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
        }

        dataset = self.modify_package_dict(dataset, {}, harvest_object)

        # Check if a dataset with the same guid exists
        existing_dataset = self._get_existing_dataset(harvest_object.guid)

        try:
            package_plugin = lib_plugins.lookup_package_plugin(
                dataset.get("type", None)
            )
            if existing_dataset:
                package_schema = package_plugin.update_package_schema()
                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    package_schema = harvester.update_package_schema_for_update(
                        package_schema
                    )
                context["schema"] = package_schema

                # Don't change the dataset name even if the title has
                dataset["name"] = existing_dataset["name"]
                dataset["id"] = existing_dataset["id"]

                harvester_tmp_dict = {}

                # check if resources already exist based on their URI
                existing_resources = existing_dataset.get("resources")
                resource_mapping = {
                    r.get("uri"): r.get("id")
                    for r in existing_resources
                    if r.get("uri")
                }
                for resource in dataset.get("resources"):
                    res_uri = resource.get("uri")
                    if res_uri and res_uri in resource_mapping:
                        resource["id"] = resource_mapping[res_uri]

                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    harvester.before_update(harvest_object, dataset, harvester_tmp_dict)

                try:
                    if dataset:
                        # Save reference to the package on the object
                        harvest_object.package_id = dataset["id"]
                        harvest_object.add()

                        p.toolkit.get_action("package_update")(context, dataset)
                    else:
                        log.info("Ignoring dataset %s" % existing_dataset["name"])
                        return "unchanged"
                except p.toolkit.ValidationError as e:
                    self._save_object_error(
                        "Update validation Error: %s" % str(e.error_summary),
                        harvest_object,
                        "Import",
                    )
                    return False

                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    err = harvester.after_update(
                        harvest_object, dataset, harvester_tmp_dict
                    )

                    if err:
                        self._save_object_error(
                            "RDFHarvester plugin error: %s" % err,
                            harvest_object,
                            "Import",
                        )
                        return False

                log.info("Updated dataset %s" % dataset["name"])

            else:
                package_schema = package_plugin.create_package_schema()
                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    package_schema = harvester.update_package_schema_for_create(
                        package_schema
                    )
                context["schema"] = package_schema

                # We need to explicitly provide a package ID
                dataset["id"] = str(uuid.uuid4())
                package_schema["id"] = [unicode_safe]

                harvester_tmp_dict = {}

                name = dataset["name"]
                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    harvester.before_create(harvest_object, dataset, harvester_tmp_dict)

                try:
                    if dataset:
                        # Save reference to the package on the object
                        harvest_object.package_id = dataset["id"]
                        harvest_object.add()

                        # Defer constraints and flush so the dataset can be indexed with
                        # the harvest object id (on the after_show hook from the harvester
                        # plugin)
                        model.Session.execute(
                            "SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED"
                        )
                        model.Session.flush()

                        p.toolkit.get_action("package_create")(context, dataset)
                    else:
                        log.info("Ignoring dataset %s" % name)
                        return "unchanged"
                except p.toolkit.ValidationError as e:
                    self._save_object_error(
                        "Create validation Error: %s" % str(e.error_summary),
                        harvest_object,
                        "Import",
                    )
                    return False

                for harvester in p.PluginImplementations(IDCATRDFHarvester):
                    err = harvester.after_create(
                        harvest_object, dataset, harvester_tmp_dict
                    )

                    if err:
                        self._save_object_error(
                            "RDFHarvester plugin error: %s" % err,
                            harvest_object,
                            "Import",
                        )
                        return False

                log.info("Created dataset %s" % dataset["name"])

        except Exception as e:
            self._save_object_error(
                "Error importing dataset %s: %r / %s"
                % (dataset.get("name", ""), e, traceback.format_exc()),
                harvest_object,
                "Import",
            )
            return False

        finally:
            model.Session.commit()

        return True
