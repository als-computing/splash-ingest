import json
import logging
import re

import numpy as np


logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def calculate_access_controls(username, projected_start_doc):
    # make an access grop list that includes the name of the proposal and the name of the beamline
    access_groups = []
    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if projected_start_doc.get('beamline'):  
        access_groups.append(projected_start_doc.get('beamline'))
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        access_groups.append(username)
        # temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub
        if projected_start_doc.get('beamline') =="bl832":
             access_groups.append("8.3.2")

    if projected_start_doc.get('proposal') and projected_start_doc.get('proposal') != 'None':
        owner_group = projected_start_doc.get('proposal')
    
    # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it


    return {"owner_group": owner_group,
            "access_groups": access_groups}


def build_search_terms(projected_start):
    ''' exctract search terms from sample name to provide something pleasing to search on '''
    terms = re.split('[^a-zA-Z0-9]', projected_start.get('sample_name'))
    description = [term.lower() for term in terms if len(term) > 0]
    return ' '.join(description)
