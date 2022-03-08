import base64
import json
import logging
from pathlib import Path
import re
from typing import Dict
from uuid import uuid4

import numpy as np
import numpy.typing as npt
from PIL import Image, ImageOps

logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return [None if np.isnan(item) else item for item in obj]
        return json.JSONEncoder.default(self, obj)


def calculate_access_controls(username, beamline, proposal) -> Dict:
    # make an access group list that includes the name of the proposal and the name of the beamline
    access_groups = []
    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if beamline:
        access_groups.append(beamline)
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        access_groups.append(username)
        # temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub
        if beamline == "bl832":
            access_groups.append("8.3.2")

    if proposal and proposal != "None":
        owner_group = proposal

    # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it
    return {"owner_group": owner_group, "access_groups": access_groups}


def build_search_terms(sample_name):
    """exctract search terms from sample name to provide something pleasing to search on"""
    terms = re.split("[^a-zA-Z0-9]", sample_name)
    description = [term.lower() for term in terms if len(term) > 0]
    return " ".join(description)


def encode_image_2_thumbnail(filename, imType="jpg"):
    logging.info(f"Creating thumbnail for dataset: {filename}")
    header = "data:image/{imType};base64,".format(imType=imType)
    with open(filename, "rb") as f:
        data = f.read()
    dataBytes = base64.b64encode(data)
    dataStr = dataBytes.decode("UTF-8")
    return header + dataStr


def build_thumbnail(image_array: npt.ArrayLike, thumbnail_dir: Path):
    image_array = image_array - np.min(image_array) + 1.001
    image_array = np.log(image_array)
    image_array = 205 * image_array / (np.max(image_array))
    auto_contrast_image = Image.fromarray(image_array.astype("uint8"))
    auto_contrast_image = ImageOps.autocontrast(auto_contrast_image, cutoff=0.1)
    filename = str(uuid4()) + ".png"
    # file = io.BytesIO()
    file = thumbnail_dir / Path(filename)
    auto_contrast_image.save(file, format="PNG")
    return file
