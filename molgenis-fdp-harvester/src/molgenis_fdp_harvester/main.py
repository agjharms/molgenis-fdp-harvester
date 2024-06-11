# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from molgenis_fdp_harvester.ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from molgenis_fdp_harvester.ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
)
from molgenis.client import Session

if __name__ == "__main__":
    harvest = DCATRDFHarvester([MolgenisEUCAIMDCATAPProfile], "EUCAIM_collections")
    # harvest.profiles = []
    harvest.gather_stage(
        "https://fdp-test.healthdata.nl/catalog/73b442ec-fb2b-4bd2-afca-b5b3ab9728c1"
    )
    # Now the import_stage
    molgenis_session = Session("http://localhost")
    molgenis_session.login("admin", "admin")
    for object in harvest._harvest_objects:
        print(object.content)
    for object in harvest._harvest_objects:
        harvest.import_stage(object, molgenis_session)
