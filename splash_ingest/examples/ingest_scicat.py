import h5py
import json
import sys
import datetime
import hashlib
import urllib
import base64
import logging
import json  # for easy parsing
import os
from pathlib import Path
from pprint import pprint

import numpy as np
import requests  # for HTTP requests
import dotenv


from splash_ingest.ingestors import MappedHD5Ingestor
from splash_ingest.model import Mapping

dotenv.load_dotenv(verbose=True)
# om .scicatbam import ScicatBam

logger = logging.getLogger("splash" + __name__)

class ScicatTomo():
    # settables
    baseurl = os.getenv('CATAMEL_URL') + "/api/v3/"
    # timeouts = (4, 8)  # we are hitting a transmission timeout...
    timeouts = None  # we are hitting a transmission timeout...
    sslVerify = True # do not check certificate
    username = os.getenv('CATAMEL_USER')
    password = os.getenv('CATAMEL_PW')
    delete_existing = False
    # You should see a nice, but abbreviated table here with the logbook contents.
    token = None # store token here
    settables = ['host', 'baseurl', 'timeouts', 'sslVerify', 'username', 'password', 'token']
    pid = 0 # gets set if you search for something
    entries = None # gets set if you search for something
    datasetType = "RawDatasets"
    datasetTypes = ["RawDatasets", "DerivedDatasets", "Proposals"]
    test = False

    def __init__(self, **kwargs):
        # nothing to do
        for key, value in kwargs.items():
            assert key in self.settables, f"key {key} is not a valid input argument"
            setattr(self, key, value)
        # get token
        self.token = self.get_token(username=self.username, password=self.password)

    def get_token(self, username=None, password=None):
        if username is None: username = self.username
        if password is None: password = self.password
        """logs in using the provided username / password combination and receives token for further communication use"""
        logger.info("Getting new token ...")

        response = requests.post(
            self.baseurl + "Users/login",
            json={"username": username, "password": password},
            timeout=self.timeouts,
            stream=False,
            verify=self.sslVerify,
        )
        if not response.ok:
            logger.error(f'** Error received: {response}')
            err = response.json()["error"]
            logger.error(f'{err["name"]}, {err["statusCode"]}: {err["message"]}')
            sys.exit(1)  # does not make sense to continue here
            data = response.json()
            logger.error(f"Response: {data}")

        data = response.json()
        # print("Response:", data)
        token = data["id"]  # not sure if semantically correct
        logger.info(f"token: {token}")
        self.token = token # store new token
        return token


    def send_to_scicat(self, url, dataDict = None, cmd="post"):
        """ sends a command to the SciCat API server using url and token, returns the response JSON
        Get token with the getToken method"""
        if cmd == "post":
            response = requests.post(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "delete":
            response = requests.delete(
                url, params={"access_token": self.token}, 
                timeout=self.timeouts, 
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "get":
            response = requests.get(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "patch":
            response = requests.patch(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        rdata = response.json()
        if not response.ok:
            err = response.json()["error"]
            logger.error(f'{err["name"]}, {err["statusCode"]}: {err["message"]}')
            logger.error("returning...")
            rdata = response.json()
            logger.error(f"Response: {json.dumps(rdata, indent=4)}")

        return rdata


    def getFileSizeFromPathObj(self, pathobj):
        filesize = pathobj.lstat().st_size
        return filesize


    def getFileChecksumFromPathObj(self, pathobj):
        with open(pathobj) as file_to_check:
            # pipe contents of the file through
            return hashlib.md5(file_to_check.read()).hexdigest()

    def clear_previous_attachments(self, datasetId, datasetType):
        # remove previous entries to avoid tons of attachments to a particular dataset. 
        # todo: needs appropriate permissions!
        self.get_entries(url = self.baseurl + "Attachments", whereDict = {"datasetId": str(datasetId)})
        for entry in self.entries:
            url = self.baseurl + f"Attachments/{urllib.parse.quote_plus(entry['id'])}"
            self.send_to_scicat(url, {}, cmd="delete")

    def add_data_block(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clear_previous_attachments(datasetId, datasetType)

        dataBlock = {
            # "id": pid,
            "size": self.get_file_size_from_path_obj(filename),
            "dataFileList": [
                {
                    "path": str(filename.absolute()),
                    "size": self.get_file_size_from_path_obj(filename),
                    "time": self.getFileModTimeFromPathObj(filename),
                    "chk": "",  # do not do remote: getFileChecksumFromPathObj(filename)
                    "uid": str(
                        filename.stat().st_uid
                    ),  # not implemented on windows: filename.owner(),
                    "gid": str(filename.stat().st_gid),
                    "perm": str(filename.stat().st_mode),
                }
            ],
            "ownerGroup": "BAM 6.5",
            "accessGroups": ["BAM", "BAM 6.5"],
            "createdBy": "datasetUpload",
            "updatedBy": "datasetUpload",
            "datasetId": datasetId,
            "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            # "createdAt": "",
            # "updatedAt": ""
        }
        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datasetId)}/origdatablocks"
        logger.debug(url)
        resp = self.send_to_scicat(url, dataBlock)
        return resp


    def get_entries(self, url, whereDict = {}):
        # gets the complete response when searching for a particular entry based on a dictionary of keyword-value pairs
        resp = self.send_to_scicat(url, {"filter": {"where": whereDict}}, cmd="get")
        self.entries = resp
        return resp


    def get_pid(self, url, whereDict = {}, returnIfNone=0, returnField = 'pid'):
        # returns only the (first matching) pid (or proposalId in case of proposals) matching a given search request
        resp = self.get_entries(url, whereDict)
        if resp == []:
            # no raw dataset available
            pid = returnIfNone
        else:
            pid = resp[0][returnField]
        self.pid = pid
        return pid
        
    def add_thumbnail(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        if clearPrevious:
            self.clear_previous_attachments(datasetId, datasetType)

        def encodeImageToThumbnail(filename, imType = 'jpg'):
            header = "data:image/{imType};base64,".format(imType=imType)
            with open(filename, 'rb') as f:
                data = f.read()
            dataBytes = base64.b64encode(data)
            dataStr = dataBytes.decode('UTF-8')
            return header + dataStr

        dataBlock = {
            "caption": filename.stem,
            "thumbnail" : encodeImageToThumbnail(filename),
            "datasetId": datasetId,
            "ownerGroup": "BAM 6.5",
        }

        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datasetId)}/attachments"
        logger.debug(url)
        resp = requests.post(
                    url,
                    params={"access_token": self.token},
                    timeout=self.timeouts,
                    stream=False,
                    json = dataBlock,
                    verify=self.sslVerify,
                )
        return resp

    def process_start_doc(self, filepath, run_start, descriptor_doc={}, thumbnail=None):
        access_groups = ['als','als832']
        created_by = 'ingestor'
        # create a sample
        sample = {
            # "sampleId": "test",
            "owner": self.username,
            "description": "string",
            "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "sampleCharacteristics": {},
            "isPublished": True,
            "ownerGroup": "ingestor",
            "accessGroups": access_groups,
            "createdBy": created_by,
            "updatedBy": created_by,
            "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z"
        }
        sample_url = f'{self.baseurl}/Samples'
        resp = self.send_to_scicat(sample_url, sample)
        # year, datasetName, lbEntry = self.getLbEntryFromFileName(filepath)
        self.year = "year"
        self.dataset_name = "creepy_spider"
        self.lb_entry = "what is an lb entry?"
        # # this sets scs.year, scs.datasetName, scs.lbEntry

        logger.info(f" working on {filepath}")

        # see if entry exists:
        pid = self.getPid( # changed from "RawDatasets" to "datasets" which should be agnostic
            self.baseurl + "datasets", {"datasetName": self.dataset_name}, returnIfNone=0
        )

        if (self.pid != 0) and self.delete_existing == False:
            # delete offending item
            url = self.scb.baseurl + "RawDatasets/{id}".format(id=urllib.parse.quote_plus(pid))
            self.scb.sendToSciCat(url, {}, cmd="delete")
            self.pid = 0

        principalInvestigator = run_start.get(':measurement:sample:experiment:pi')
        if not principalInvestigator:
            principalInvestigator = "uknonwn"
        creationLocation = run_start.get(':measurement:instrument:source:beamline')
        if not creationLocation:
            creationLocation = "unknown"
        creationTime = run_start.get(':process:acquisition:start_date')
        if not creationTime:
            creationTime = "unkown"
        else:
            creationTime = datetime.datetime.isoformat(datetime.datetime.fromtimestamp(creationTime[0])) + "Z"
        data = {  # model for the raw datasets as defined in the RawDatasets
            "owner": None,
            "contactEmail": "dmcreynolds@lbl.gov",
            "createdBy": self.username,
            "updatedBy": self.username,
            "creationLocation": creationLocation,
            "creationTime": None,
            "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "creationTime": creationTime,
            "datasetName": filepath.stem,
            "type": "raw",
            "instrumentId": run_start.get(':measurement:instrument:instrument_name'),
            "owner": self.username,
            "ownerGroup": "ingestor",
            "accessGroups": access_groups,#run_start.get('data_groups'),
            "proposalId": run_start.get(':measurement:sample:experiment:proposal'),
            "dataFormat": "DX",
            "principalInvestigator": principalInvestigator,
            # "pid": run_start['uid'],
            "size": 0,
            "sourceFolder": filepath.parent.as_posix(),
            "size": self.getFileSizeFromPathObj(filepath),
            "scientificMetadata": self.extract_scientific_metadata(descriptor_doc),
            "sampleId": run_start.get(':measurement:sample:uuid'),
            "isPublished": False,
            
        }
        urlAdd = "RawDatasets"
        encoded_data = json.loads(json.dumps(data, cls=NPArrayEncoder))
        # determine thumbnail: 
        # upload
        print(thumbnail)
        if thumbnail.exists():
            npid = self.uploadBit(pid = self.pid, urlAdd = urlAdd, data = encoded_data, attachFile = thumbnail)
        logger.info("* * * * adding datablock")
        datasetType="RawDatasets"
        dataBlock = {
            # "id": npid,
            "size": self.getFileSizeFromPathObj(filepath),
            "dataFileList": [
                {
                    "path": str(filepath.absolute()),
                    "size": self.getFileSizeFromPathObj(filepath),
                    "time": self.getFileModTimeFromPathObj(filepath),
                    "chk": "",  # do not do remote: getFileChecksumFromPathObj(filename)
                    "uid": str(
                        filepath.stat().st_uid
                    ),  # not implemented on windows: filename.owner(),
                    "gid": str(filepath.stat().st_gid),
                    "perm": str(filepath.stat().st_mode),
                }
            ],
            "ownerGroup": "ingestor",
            "accessGroups": ['als', 'als832'],
            "createdBy": "datasetUpload",
            "updatedBy": "datasetUpload",
            "datasetId": npid,
            "updatedAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            "createdAt": datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z",
            # "createdAt": "",
            # "updatedAt": ""
        }
        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(npid)}/origdatablocks"
        logging.debug(url)
        resp = self.sendToSciCat(url, dataBlock)
        print(resp)

    
    @staticmethod
    def extract_scientific_metadata(descriptor):
        retrun_dict = {k.replace(":", "/"): v for k,v in descriptor['configuration']['all']['data'].items()}
        return retrun_dict

    @staticmethod
    def getFileModTimeFromPathObj(pathobj):
        # may only work on WindowsPath objects...
        # timestamp = pathobj.lstat().st_mtime
        return str(datetime.datetime.fromtimestamp(pathobj.lstat().st_mtime))

    def getEntries(self, url, whereDict = {}):
        # gets the complete response when searching for a particular entry based on a dictionary of keyword-value pairs
        resp = self.sendToSciCat(url, {"filter": {"where": whereDict}}, cmd="get")
        self.entries = resp
        return resp

    def getPid(self, url, whereDict = {}, returnIfNone=0, returnField = 'pid'):
        # returns only the (first matching) pid (or proposalId in case of proposals) matching a given search request
        resp = self.getEntries(url, whereDict)
        if resp == []:
            # no raw dataset available
            pid = returnIfNone
        else:
            pid = resp[0][returnField]
        self.pid = pid
        return pid

    def sendToSciCat(self, url, dataDict = None, cmd="post"):
        """ sends a command to the SciCat API server using url and token, returns the response JSON
        Get token with the getToken method"""
        print(f'access token {self.token}')
        if cmd == "post":
            response = requests.post(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "delete":
            response = requests.delete(
                url, params={"access_token": self.token}, 
                timeout=self.timeouts, 
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "get":
            response = requests.get(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "patch":
            response = requests.patch(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        rdata = response.json()
        if not response.ok:
            err = response.json()["error"]
            logging.error(f'{err["name"]}, {err["statusCode"]}: {err["message"]}')
            logging.error("returning...")
            rdata = response.json()
            logging.error(f"Response: {json.dumps(rdata, indent=4)}")

        return rdata

    def uploadBit(self, pid = 0, urlAdd = None, data = None, attachFile = None):
        # upload the bits to the database
        # try sending it...
        if not self.test and pid == 0: # and not self.uploadType in ["samples"]:
            logger.info("* * * * creating new entry")
            url = self.baseurl + f"{urlAdd}/replaceOrCreate"
            resp = self.sendToSciCat(url, data)
            if "pid" in resp:
                npid = resp["pid"]
            elif "id" in resp:
                npid = resp["id"]
            elif "proposalId" in resp:
                npid = resp["proposalId"]
    
        elif not self.test and pid != 0: # and not adict["uploadType"] in ["samples"]:
            logger.info("* * * * updating existing entry")
            url = self.baseurl + f"{urlAdd}/{urllib.parse.quote_plus(pid)}"
            resp = self.sendToSciCat(url, data, "patch")
            npid = pid
        
        if attachFile is not None:
            # attach an additional file as "thumbnail"
            assert isinstance(attachFile, Path), 'attachFile must be an instance of pathlib.Path'

            if attachFile.exists():
                logger.info("attaching thumbnail {} to {} \n".format(attachFile, npid))
                urlAddThumbnail = urlAdd
                if urlAdd == "DerivedDatasets": 
                    urlAddThumbnail="datasets" # workaround for scicat inconsistency
                resp = self.addThumbnail(npid, attachFile, datasetType = urlAddThumbnail, clearPrevious=True)
                logger.debug(resp.json())
        return npid


    def addThumbnail(self, datasetId = None, filename = None, datasetType="RawDatasets", clearPrevious = False):
        # if clearPrevious:
        #     self.clearPreviousAttachments(datasetId, datasetType)

        def encodeImageToThumbnail(filename, imType = 'jpg'):
            header = "data:image/{imType};base64,".format(imType=imType)
            with open(filename, 'rb') as f:
                data = f.read()
            dataBytes = base64.b64encode(data)
            dataStr = dataBytes.decode('UTF-8')
            return header + dataStr

        dataBlock = {
            "caption": filename.stem,
            "thumbnail" : encodeImageToThumbnail(filename),
            "datasetId": datasetId,
            "ownerGroup": "ingestor",
        }

        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datasetId)}/attachments"
        logging.debug(url)
        resp = requests.post(
                    url,
                    params={"access_token": self.token},
                    timeout=self.timeouts,
                    stream=False,
                    json = dataBlock,
                    verify=self.sslVerify,
                )
        return resp


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def gen_ev_docs(scm: ScicatTomo, mapping_file, h5_file_path, data_groups):
    with open(mapping_file, 'r') as json_file:
        data = json.load(json_file)
    map = Mapping(**data)
    with h5py.File(h5_file_path, 'r') as h5_file:
        ingestor = MappedHD5Ingestor(
            map, 
            h5_file, 
            'root', 
            thumbs_root=os.getenv('THUMBNAILS_ROOT'),
            data_groups=[data_groups])
        for name, doc in ingestor.generate_docstream():
            if 'start' in name:
                start_doc = doc
                continue
            if 'descriptor' in name:
                descriptor = doc
                # pprint(doc)
        pprint(ingestor.issues)
        # descriptor.map()
        thumbnail = 0
        if len(ingestor.thumbnails) > 0:
            thumbnail = ingestor.thumbnails[0]
        print(ingestor.thumbnails)
        scm.process_start_doc(Path(h5_file_path), start_doc, descriptor_doc=descriptor, thumbnail=thumbnail)



def main():
    if len(sys.argv) != 4:
       print("Usage: <command> mapping.json data.h5 data_groups", sys.argv)
       return

    mapping_file = sys.argv[1]
    h5_file = sys.argv[2]
    data_groups = sys.argv[3]
    scm = ScicatTomo()
    gen_ev_docs(scm, mapping_file, h5_file, data_groups)

            
if __name__ == "__main__":
    main()
