# SciCat Ingestion
At present, the ingestion workflow depends on Databroker. It accepts a stream of event_model documents and projects the start document using the projection found in the mapping file provided. See [example file](../mappings/832Mapping.json).

## Backgroud: Access Controls in SciCat
The SciCat ingestor maps data from data sets to SciCat's access control scheme.

In SciCat, many collections (e.g. Dataset, OrigDatablock, Attachment) contain two fields that affect access controls in the application: `ownerGroup` and `accessGroups`.

In both cases, the scicat ingestor populates these fields based on information from the dataset.

### ownerGroup
Determines users that can read and write objects in SciCat. It is a single string that is compared with the `accessGroups` field in the logged-in user's profile. In the case of "functional users", owner group is matched against the user's login name.

Some of the features that owners enjoy include being able add labels attachments to Datasets.

### accessGroup
Determines users that can view objects in SciCat (including having them return in searches). It is a list of strings. Like [ownerGroup](ownergroup), it is compared against the `accessGroups` field of the logged-in user's profile. If one of the strings in the in the user's profile matches one of the strings in the object's `accessGroups` field, the user can view the object.

## Access controls for ALS Ingestion
Currently, the [SciCat ingestion](../splash_ingest/scicat.py) calculates access controls (SciCat `ownerGroup` and `accessGroups` fields on Sample, Dataset, OrigDatablock and Attachment by reading data from the dataset file and applying various rules.

### ownerGroup

#### proposal number in dataset
If the proposal number is found in the incoming dataset, the ingestor sets it as the ownerGroup. This means that anyone user who logs into the system and is designated a member of that proposal or any ESAF for that proposal will enjoy the privileges of an owner.

#### no proposal number
If a proposal number is not found in the incoming dataset, the ingestor will add the identity of the user that ingested the dataset. Beamline users will not enjoy write privilages, but will be able to view the ingested SciCat objects.

### accessGroups
If the beamline identifier is found in the incoming dataset, it is added to the `accessGroups` so that ALS Staff can associated with that beamline (via a call to ALSHub on login) can view the ingest SciCat objects.

