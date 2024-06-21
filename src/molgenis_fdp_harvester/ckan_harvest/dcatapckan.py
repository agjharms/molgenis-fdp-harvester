# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://raw.githubusercontent.com/ckan/ckanext-dcat/master/ckanext/dcat/profiles/euro_dcat_ap.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

import json

from rdflib import term

# import ckantoolkit as toolkit

# from ckan.lib.munge import munge_tag

# from ckanext.dcat.utils import (
#     resource_uri,
#     DCAT_EXPOSE_SUBCATALOGS,
#     DCAT_CLEAN_TAGS,
#     publisher_uri_organization_fallback,
# )
from .baseparser import RDFProfile, munge_tag
from .baseparser import (
    DCAT,
    DCT,
    ADMS,
    FOAF,
    OWL,
    SPDX,
)

# config = toolkit.config
DCAT_CLEAN_TAGS = False
NORMALIZE_CKAN_FORMAT = True


DISTRIBUTION_LICENSE_FALLBACK_CONFIG = "ckanext.dcat.resource.inherit.license"


class EuropeanDCATAPProfile(RDFProfile):
    """
    An RDF profile based on the DCAT-AP for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """

    def parse_dataset(self, dataset_dict, dataset_ref):
        # dataset_dict["extras"] = []
        # dataset_dict["resources"] = []

        # Basic fields
        for key, predicate in (
            ("title", DCT.title),
            ("notes", DCT.description),
            ("url", DCAT.landingPage),
            ("version", OWL.versionInfo),
        ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict[key] = value

        if not dataset_dict.get("version"):
            # adms:version was supported on the first version of the DCAT-AP
            value = self._object_value(dataset_ref, ADMS.version)
            if value:
                dataset_dict["version"] = value

        # Tags
        # replace munge_tag to noop if there's no need to clean tags
        # do_clean = toolkit.asbool(config.get(DCAT_CLEAN_TAGS, False))
        do_clean = DCAT_CLEAN_TAGS
        tags_val = [
            munge_tag(tag) if do_clean else tag for tag in self._keywords(dataset_ref)
        ]
        tags = [{"name": tag} for tag in tags_val]
        dataset_dict["tags"] = tags

        # Extras

        #  Simple values
        for key, predicate in (
            ("issued", DCT.issued),
            ("modified", DCT.modified),
            ("identifier", DCT.identifier),
            ("version_notes", ADMS.versionNotes),
            ("frequency", DCT.accrualPeriodicity),
            ("provenance", DCT.provenance),
            ("dcat_type", DCT.type),
        ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict["extras"].append({"key": key, "value": value})

        #  Lists
        for (
            key,
            predicate,
        ) in (
            ("language", DCT.language),
            ("theme", DCAT.theme),
            ("alternate_identifier", ADMS.identifier),
            ("conforms_to", DCT.conformsTo),
            ("documentation", FOAF.page),
            ("related_resource", DCT.relation),
            ("has_version", DCT.hasVersion),
            ("is_version_of", DCT.isVersionOf),
            ("source", DCT.source),
            ("sample", ADMS.sample),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict["extras"].append({"key": key, "value": json.dumps(values)})

        # Contact details
        contact = self._contact_details(dataset_ref, DCAT.contactPoint)
        if not contact:
            # adms:contactPoint was supported on the first version of DCAT-AP
            contact = self._contact_details(dataset_ref, ADMS.contactPoint)

        if contact:
            for key in ("uri", "name", "email"):
                if contact.get(key):
                    dataset_dict["extras"].append(
                        {"key": "contact_{0}".format(key), "value": contact.get(key)}
                    )

        # Publisher
        publisher = self._publisher(dataset_ref, DCT.publisher)
        for key in ("uri", "name", "email", "url", "type"):
            if publisher.get(key):
                dataset_dict["extras"].append(
                    {"key": "publisher_{0}".format(key), "value": publisher.get(key)}
                )

        # Temporal
        start, end = self._time_interval(dataset_ref, DCT.temporal)
        if start:
            dataset_dict["extras"].append({"key": "temporal_start", "value": start})
        if end:
            dataset_dict["extras"].append({"key": "temporal_end", "value": end})

        # Spatial
        spatial = self._spatial(dataset_ref, DCT.spatial)
        for key in ("uri", "text", "geom"):
            self._add_spatial_to_dict(dataset_dict, key, spatial)

        # Dataset URI (explicitly show the missing ones)
        dataset_uri = str(dataset_ref) if isinstance(dataset_ref, term.URIRef) else ""
        dataset_dict["extras"].append({"key": "uri", "value": dataset_uri})

        # access_rights
        access_rights = self._access_rights(dataset_ref, DCT.accessRights)
        if access_rights:
            dataset_dict["extras"].append(
                {"key": "access_rights", "value": access_rights}
            )

        # License
        if "license_id" not in dataset_dict:
            dataset_dict["license_id"] = self._license(dataset_ref)

        # Source Catalog
        # FIXME nulled out for now
        if False:
            if toolkit.asbool(config.get(DCAT_EXPOSE_SUBCATALOGS, False)):
                catalog_src = self._get_source_catalog(dataset_ref)
                if catalog_src is not None:
                    src_data = self._extract_catalog_dict(catalog_src)
                    dataset_dict["extras"].extend(src_data)

        # Resources
        for distribution in self._distributions(dataset_ref):
            resource_dict = {}

            #  Simple values
            for key, predicate in (
                ("name", DCT.title),
                ("description", DCT.description),
                ("access_url", DCAT.accessURL),
                ("download_url", DCAT.downloadURL),
                ("issued", DCT.issued),
                ("modified", DCT.modified),
                ("status", ADMS.status),
                ("license", DCT.license),
            ):
                value = self._object_value(distribution, predicate)
                if value:
                    resource_dict[key] = value

            resource_dict["url"] = self._object_value(
                distribution, DCAT.downloadURL
            ) or self._object_value(distribution, DCAT.accessURL)
            #  Lists
            for key, predicate in (
                ("language", DCT.language),
                ("documentation", FOAF.page),
                ("conforms_to", DCT.conformsTo),
            ):
                values = self._object_value_list(distribution, predicate)
                if values:
                    resource_dict[key] = json.dumps(values)

            # rights
            rights = self._access_rights(distribution, DCT.rights)
            if rights:
                resource_dict["rights"] = rights

            # Format and media type
            normalize_ckan_format = NORMALIZE_CKAN_FORMAT
            # normalize_ckan_format = toolkit.asbool(
            #     config.get("ckanext.dcat.normalize_ckan_format", True)
            # )
            imt, label = self._distribution_format(distribution, normalize_ckan_format)

            if imt:
                resource_dict["mimetype"] = imt

            if label:
                resource_dict["format"] = label
            elif imt:
                resource_dict["format"] = imt

            # Size
            size = self._object_value_int(distribution, DCAT.byteSize)
            if size is not None:
                resource_dict["size"] = size

            # Checksum
            for checksum in self.g.objects(distribution, SPDX.checksum):
                algorithm = self._object_value(checksum, SPDX.algorithm)
                checksum_value = self._object_value(checksum, SPDX.checksumValue)
                if algorithm:
                    resource_dict["hash_algorithm"] = algorithm
                if checksum_value:
                    resource_dict["hash"] = checksum_value

            # Distribution URI (explicitly show the missing ones)
            resource_dict["uri"] = (
                str(distribution) if isinstance(distribution, term.URIRef) else ""
            )

            # Remember the (internal) distribution reference for referencing in
            # further profiles, e.g. for adding more properties
            resource_dict["distribution_ref"] = str(distribution)

            dataset_dict["resources"].append(resource_dict)

        # TODO: remove, dead code
        if self.compatibility_mode:
            # Tweak the resulting dict to make it compatible with previous
            # versions of the ckanext-dcat parsers
            for extra in dataset_dict["extras"]:
                if extra["key"] in (
                    "issued",
                    "modified",
                    "publisher_name",
                    "publisher_email",
                ):
                    extra["key"] = "dcat_" + extra["key"]

                if extra["key"] == "language":
                    extra["value"] = ",".join(sorted(json.loads(extra["value"])))

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")
