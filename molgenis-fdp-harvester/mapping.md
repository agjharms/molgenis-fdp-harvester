# EUCAIM FDP Harvester Mapping

Note: this repository is a proof-of-concept only.

Properties marked with a # will have to use stub values as they are not part of FDP standard.
Properties with a ? could be defined in DCAT but are not mandatory.

COLUMN                  DESCRIPTION                                 EXAMPLE
id                      harvester guid                              abcdefgh
name                    title                                       Dataset Title
description             description of the dataset                  Dataset Description
intended_purpose?       The intended purpose of the dataset         Dataset purpose
provider#               Provider of the dataset                     CHAIMELEON
order_of_magnitude#     how many subjects                           2,
biobank#                overarching collection                      CHAI-4
country#                originating country from dataset            EU
collection_method#      eg Case Control, Cohort, etc.               OTHER
type#                   annotated/original/processed                ORIGINAL_DATASETS
imaging_modality#       what modality                               MR
image_access_type#      open/closed/request access                  BY_REQUEST

Commands:

```
my_table = session.get("EUCAIM_collections")

session.add("EUCAIM_collections", data={'id': 'peerid', 'name': "neeeem", "description": "beschrijving", "biobank": "CHAI-4", "country": "EU", "collection_method": "OTHER", "type": "ORIGINAL_DATASETS", "imag
    ...: ing_modality": "MR", "image_access_type": "BY_REQUEST", "order_of_magnitude": 2, "intended_purpose": "lekker snacken", "provider": "CHAIMELEON"})
```
