from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class MappingField(BaseModel):
    field: str = Field(title='name of the field that is placed in the start document')
    description: Optional[str] = Field(title='description ')


class ConfigurationMapping(BaseModel):
    device: str
    fields: List[MappingField]


class StreamMappingField(MappingField):
    external: Optional[bool] = Field(title="Indicates whether field will be represented in the event directly or from an external file")


class StreamMapping(BaseModel):
    fields: List[MappingField]
    time_stamp: str = Field(title='time_stamp field', description='field to use to get time stamp values')
    conf_mappings: Optional[List[ConfigurationMapping]] = Field(title="event descriptor confguration")


class Mapping(BaseModel):
    name: str = Field(title='Mapping name', description='Name of this mapping')
    description: str = Field(title='Mapping description', description='Description of this mapping')
    resource_spec: str = Field(title='Resource spec', description='databroker.handler spec for the resource document' +
                                                                  'produced by the ingestor. e.g. HDF, TIFFStack, etc.')
    md_mappings: Optional[List[MappingField]]
    stream_mappings: Optional[Dict[str, StreamMapping]]
    projections: Optional[List[Dict]]
