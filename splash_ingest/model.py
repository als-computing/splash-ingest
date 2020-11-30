from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class MDMapping(BaseModel):
    field: str = Field(title='name of the field that is placed in the start document')
    description: Optional[str] = Field(title='description ')


class StreamMappingField(BaseModel):
    field: str = Field(title='name of the field')
    external: Optional[bool] = Field(title="Indicates whether field will be represented in the event directly or from an external file")
    description: Optional[str] = Field(title="description of this field")


class StreamMapping(BaseModel):
    mapping_fields: List[StreamMappingField]
    time_stamp: str = Field(title='time_stamp field', description='field to use to get time stamp values')
    thumbnail: Optional[bool] = Field(title="determines whether to treat stream as thumbnail")


class Mapping(BaseModel):
    name: str = Field(title='Mapping name', description='Name of this mapping')
    description: str = Field(title='Mapping description', description='Description of this mapping')
    resource_spec: str = Field(title='Resource spec', description='databroker.handler spec for the resource document' +
                                                                  'produced by the ingestor. e.g. HDF, TIFFStack, etc.')
    md_mappings: Optional[List[MDMapping]]
    stream_mappings: Optional[Dict[str, StreamMapping]]
    projections: Optional[List[Dict]]
