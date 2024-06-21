# SPDX-FileCopyrightText: Open Knowlege
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileContributor: Stichting Health-RI

# This material is copyright (c) Open Knowledge.
# It is open and licensed under the GNU Affero General Public License (AGPL) v3.0
# Original location of file: https://raw.githubusercontent.com/ckan/ckanext-dcat/master/ckanext/dcat/profiles/euro_dcat_ap.py
#
# Modified by Stichting Health-RI to remove dependencies on CKAN

from typing import Dict

from rdflib import URIRef

import logging


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
    DCT,
)

log = logging.getLogger(__name__)

# config = toolkit.config
DCAT_CLEAN_TAGS = False
NORMALIZE_CKAN_FORMAT = True


DISTRIBUTION_LICENSE_FALLBACK_CONFIG = "ckanext.dcat.resource.inherit.license"


class MolgenisEUCAIMDCATAPProfile(RDFProfile):
    """
    An RDF profile based on the DCAT-AP for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """

    def parse_dataset(self, dataset_dict: Dict, dataset_ref: URIRef):
        # dataset_dict["extras"] = []
        # dataset_dict["resources"] = []
        dataset_dict["uri"] = str(dataset_ref)
        # Basic fields
        for key, predicate in (
            ("name", DCT.title),
            ("description", DCT.description),
        ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict[key] = value

        # TODO store keywords somewhere
        # replace munge_tag to noop if there's no need to clean tags
        do_clean = DCAT_CLEAN_TAGS
        tags_val = [
            munge_tag(tag) if do_clean else tag for tag in self._keywords(dataset_ref)
        ]
        tags = [{"name": tag} for tag in tags_val]
        # dataset_dict["tags"] = tags

        # These values are fake. They need to be made "real"
        log.warn("Filling in fake values")
        dataset_dict["biobank"] = "CHAI-4"
        dataset_dict["provider"] = "CHAIMELEON"
        dataset_dict["order_of_magnitude"] = 1
        dataset_dict["country"] = "EU"
        dataset_dict["collection_method"] = "OTHER"
        dataset_dict["type"] = "ORIGINAL_DATASETS"
        dataset_dict["imaging_modality"] = "MR"
        dataset_dict["image_access_type"] = "BY_REQUEST"
        dataset_dict["intended_purpose"] = "placeholder"

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        raise NotImplementedError("FDP export is handled by MOLGENIS")
