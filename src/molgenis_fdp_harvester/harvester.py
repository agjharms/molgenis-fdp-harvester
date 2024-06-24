# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import click

from molgenis_fdp_harvester.ckan_harvest.dcatrdfharvester import DCATRDFHarvester
from molgenis_fdp_harvester.ckan_harvest.molgenis_dcat_profile import (
    MolgenisEUCAIMDCATAPProfile,
    MolgenisEIBIRDCATAPProfile,
)
from molgenis.client import Session


@click.command()
@click.option("--fdp", help="FAIR Data Point catalog to harvest", required=True)
@click.option("--host", help="MOLGENIS host to harvest to", required=False)
@click.option(
    "--entity",
    help="Entity of MOLGENIS host to harvest to (e.g. EUCAIM_collections)",
    required=False,
)
@click.option(
    "--username", help="Username of MOLGENIS host to harvest to", required=False
)
@click.password_option(confirmation_prompt=False, required=False)
def cli(
    fdp: str,
    host: str,
    entity: str,
    username: str,
    password: str,
):
    molgenis_session = Session(host)
    molgenis_session.login(username, password)

    harvest = DCATRDFHarvester([MolgenisEIBIRDCATAPProfile], entity)

    harvest.gather_stage(fdp)
    # for object in harvest._harvest_objects:
    #     print(object.content)
    harvest.fetch_stage(molgenis_session)
    for object in harvest._harvest_objects:
        harvest.import_stage(object, molgenis_session)


if __name__ == "__main__":
    cli()
