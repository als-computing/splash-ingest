from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import h5py
from pyscicat.client import ScicatClient
from pyscicat.model import (
    Attachment,
    Datablock,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
)

from splash_ingest.ingestors.scicat_utils import (
    build_search_terms,
    build_thumbnail,
    calculate_access_controls,
    encode_image_2_thumbnail,
    NPArrayEncoder,
)
from splash_ingest.ingestors.utils import Issue, Severity

ingest_spec = "als832_dx_3"

logger = logging.getLogger("scicat_ingest")


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: str,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    with h5py.File(file_path, "r") as file:
        file_path = Path(file_path)
        scicat_metadata = _extract_fields(file, scicat_metadata_keys, issues)
        scientific_metadata = _extract_fields(file, scientific_metadata_keys, issues)
        scientific_metadata["data_sample"] = _get_data_sample(file)
        encoded_scientific_metadata = json.loads(
            json.dumps(scientific_metadata, cls=NPArrayEncoder)
        )
        access_controls = calculate_access_controls(
            username,
            scicat_metadata.get("/measurement/sample/experiment/beamline"),
            scicat_metadata.get("/measurement/sample/experiment/proposal"),
        )
        logger.info(
            f"Access controls for  {file_path}  access_groups: {access_controls.get('accessroups')} "
            f"owner_group: {access_controls.get('owner_group')}"
        )

        ownable = Ownable(
            ownerGroup=access_controls["owner_group"],
            accessGroups=access_controls["access_groups"],
        )
        dataset_id = upload_raw_dataset(
            scicat_client,
            file_path,
            scicat_metadata,
            encoded_scientific_metadata,
            ownable,
        )
        upload_data_block(scicat_client, file_path, dataset_id, ownable)

        thumbnail_file = build_thumbnail(file["/exchange/data"][0], thumbnail_dir)
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)

        return dataset_id


def upload_raw_dataset(
    scicat_client: ScicatClient,
    file_path: Path,
    scicat_metadata: Dict,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    file_size = get_file_size(file_path)
    file_mod_time = get_file_mod_time(file_path)
    file_name = scicat_metadata.get("/measurement/sample/file_name")
    description = build_search_terms(file_name)
    appended_keywords = description.split()

    dataset = RawDataset(
        owner=scicat_metadata.get("/measurement/sample/experiment/pi") or "Unknown",
        contactEmail=scicat_metadata.get("/measurement/sample/experimenter/email")
        or "Unknown",
        creationLocation=scicat_metadata.get("/measurement/instrument/instrument_name")
        or "Unknown",
        datasetName=file_name,
        type=DatasetType.raw,
        instrumentId=scicat_metadata.get("/measurement/instrument/instrument_name")
        or "Unknown",
        proposalId=scicat_metadata.get("/measurement/sample/experiment/proposal"),
        dataFormat="DX",
        principalInvestigator=scicat_metadata.get("/measurement/sample/experiment/pi")
        or "Unknown",
        sourceFolder=str(file_path.parent),
        size=file_size,
        scientificMetadata=scientific_metadata,
        sampleId=description,
        isPublished=False,
        description=description,
        keywords=appended_keywords,
        creationTime=file_mod_time,
        **ownable.dict(),
    )
    dataset_id = scicat_client.upload_raw_dataset(dataset)
    return dataset_id


def create_data_files(file_path: Path) -> List[DataFile]:
    "Collects all fits files"
    datafiles = []
    datafile = DataFile(
        path=file_path.name,
        size=get_file_size(file_path),
        time=get_file_mod_time(file_path),
        type="RawDatasets",
    )
    datafiles.append(datafile)
    return datafiles


def upload_data_block(
    scicat_client: ScicatClient, file_path: Path, dataset_id: str, ownable: Ownable
) -> Datablock:
    "Creates a datablock of fits files"
    datafiles = create_data_files(file_path)

    datablock = Datablock(
        datasetId=dataset_id,
        size=get_file_size(file_path),
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(datablock)


def upload_attachment(
    scicat_client: ScicatClient,
    encoded_thumnbnail: str,
    dataset_id: str,
    ownable: Ownable,
) -> Attachment:
    "Creates a thumbnail png"
    attachment = Attachment(
        datasetId=dataset_id,
        thumbnail=encoded_thumnbnail,
        caption="scattering image",
        **ownable.dict(),
    )
    scicat_client.upload_attachment(attachment)


def get_file_size(file_path: Path) -> int:
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    return str(datetime.fromtimestamp(file_path.lstat().st_mtime))


def _extract_fields(file, keys, issues) -> Dict[str, Any]:
    metadata = {}
    for md_key in keys:
        dataset = file.get(md_key)
        if not dataset:
            issues.append(
                Issue(msg=f"dataset not found {md_key}", severity=Severity.warning)
            )
            continue
        metadata[md_key] = _get_dataset_value(file[md_key])
    return metadata


def _get_dataset_value(data_set):
    logger.debug(f"{data_set}  {data_set.dtype}")
    try:
        if "S" in data_set.dtype.str:
            if data_set.shape == (1,):
                return data_set.asstr()[0]
            elif data_set.shape == ():
                return data_set[()].decode("utf-8")
            else:
                return list(data_set.asstr())
        else:
            if data_set.maxshape == (1,):
                logger.debug(f"{data_set}  {data_set[()][0]}")
                return data_set[()][0]
            else:
                logger.debug(f"{data_set}  {data_set[()]}")
                return data_set[()]
    except Exception:
        logger.exception("Exception extracting dataset value")
        return None


def _get_data_sample(file, sample_size=10):
    data_sample = {}
    for key in data_sample_keys:
        data_array = file.get(key)
        if not data_array:
            continue
        step_size = int(len(data_array) / sample_size)
        if step_size == 0:
            step_size = 1
        sample = data_array[0::step_size]
        data_sample[key] = sample

    return data_sample


scicat_metadata_keys = [
    "/measurement/instrument/instrument_name",
    "/measurement/sample/experiment/beamline",
    "/measurement/sample/experiment/experiment_lead",
    "/measurement/sample/experiment/pi",
    "/measurement/sample/experiment/proposal",
    "/measurement/sample/experimenter/email",
    "/measurement/sample/experimenter/name",
    "/measurement/sample/file_name",
]

scientific_metadata_keys = [
    "/measurement/instrument/attenuator/setup/filter_y",
    "/measurement/instrument/camera_motor_stack/setup/tilt_motor",
    "/measurement/instrument/detection_system/objective/camera_objective",
    "/measurement/instrument/detection_system/scintillator/scintillator_type",
    "/measurement/instrument/detector/binning_x",
    "/measurement/instrument/detector/binning_y",
    "/measurement/instrument/detector/dark_field_value",
    "/measurement/instrument/detector/delay_time",
    "/measurement/instrument/detector/dimension_x",
    "/measurement/instrument/detector/dimension_y",
    "/measurement/instrument/detector/model",
    "/measurement/instrument/detector/pixel_size",
    "/measurement/instrument/detector/temperature",
    "/measurement/instrument/monochromator/setup/Z2",
    "/measurement/instrument/monochromator/setup/temperature_tc2",
    "/measurement/instrument/monochromator/setup/temperature_tc3",
    "/measurement/instrument/slits/setup/hslits_A_Door",
    "/measurement/instrument/slits/setup/hslits_A_Wall",
    "/measurement/instrument/slits/setup/hslits_center",
    "/measurement/instrument/slits/setup/hslits_size",
    "/measurement/instrument/slits/setup/vslits_Lead_Flag",
    "/measurement/instrument/source/source_name",
    "/process/acquisition/dark_fields/dark_num_avg_of",
    "/process/acquisition/dark_fields/num_dark_fields",
    "/process/acquisition/flat_fields/i0_move_x",
    "/process/acquisition/flat_fields/i0_move_y",
    "/process/acquisition/flat_fields/i0cycle",
    "/process/acquisition/flat_fields/num_flat_fields",
    "/process/acquisition/flat_fields/usebrightexpose",
    "/process/acquisition/mosaic/tile_xmovedist",
    "/process/acquisition/mosaic/tile_xnumimg",
    "/process/acquisition/mosaic/tile_xorig",
    "/process/acquisition/mosaic/tile_xoverlap",
    "/process/acquisition/mosaic/tile_ymovedist",
    "/process/acquisition/mosaic/tile_ynumimg",
    "/process/acquisition/mosaic/tile_yorig",
    "/process/acquisition/mosaic/tile_yoverlap",
    "/process/acquisition/name",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/multiRev",
    "/process/acquisition/rotation/nhalfCir",
    "/process/acquisition/rotation/num_angles",
    "/process/acquisition/rotation/range",
]

data_sample_keys = [
    "/measurement/instrument/sample_motor_stack/setup/axis1pos",
    "/measurement/instrument/sample_motor_stack/setup/axis2pos",
    "/measurement/instrument/sample_motor_stack/setup/sample_x",
    "/measurement/instrument/sample_motor_stack/setup/axis5pos",
    "/measurement/instrument/camera_motor_stack/setup/camera_elevation",
    "/measurement/instrument/source/current",
    "/measurement/instrument/camera_motor_stack/setup/camera_distance",
    "/measurement/instrument/source/beam_intensity_incident",
    "/measurement/instrument/monochromator/energy",
    "/measurement/instrument/detector/exposure_time",
    "/measurement/instrument/time_stamp",
    "/measurement/instrument/monochromator/setup/turret2",
    "/measurement/instrument/monochromator/setup/turret1",
]
