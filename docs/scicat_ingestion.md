# SciCat Ingestion
The splash_ingest service can ingest Datasets and related objects (Attachments, OrigDatablocks, Samples). In this process, it updates fields on those objects that affect access controls for those objects that affect who can read and update them within the SciCat application.

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

# Examples
In the following examples, most fields from the User and Dataset objects have been ommitted for brevity.

User1 is on ALS-X000-001:
``` json

{
    "name": "User1",
    "accessGroups": ["ALS-X000", "ALS-X000-001"]
}
```

User2 is on ALS-X000-001:
``` json

{
    "name": "User2",
    "accessGroups": ["ALS-X0001"]
}
```

BeamlineScientist is on at beamline bl0.0.0:
``` json

{
    "name": "BeamlineScientist",
    "accessGroups": ["bl0.0.0"]
}
```

A Dataset is ingested at beamline 0.0.0 for proposal ALS-X000:
``` json

{
    "proposal": "ALS-X000",
    "ownerGroup": "ALS-X000",
    "accessGroups": ["bl0.0.0"]
}
```

When User1 logs in, the Dataset will be in their list of available Datasets. AUser will also be able to add Labels and Upload additional attachments to the Dataset.

When User2 logs in, the Dataset will not appear in their list or be available in searches.

When BeamlineScientist logs in, the Dataset will be in their list of available Datasets. They will not be able to add labels or additional attachments.
