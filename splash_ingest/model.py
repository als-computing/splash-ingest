from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class StreamMapping(BaseModel):
    mapping_fields: Dict[str, str]
    time_stamp: str = Field(title='time_stamp field', description='field to use to get time stamp values')


class Mapping(BaseModel):
    name: str = Field(title='Mapping name', description='Name of this mapping')
    description: str = Field(title='Mapping description', description='Description of this mapping')
    version: str = Field(title='Mapping version', description='Version of this mapping')
    resource_spec: str = Field(title='Resource spec', description='databroker.handler spec for the resource documnet produced by the ingestor. e.g. HDF, TIFFStack, etc.')
    metadata_mappings: Dict[str, str]
    stream_mappings: Dict[str, StreamMapping]


class EMDescriptor(BaseModel):
    dtype: str
    source: str
    shape: List[int]
    units: Optional[str]
    external: Optional[str]
