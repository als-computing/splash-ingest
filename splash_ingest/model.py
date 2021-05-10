from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

SCHEMA_VERSION = 2


class MappingField(BaseModel):
    field: str = Field(title='name of the field that is placed in the start document')
    description: Optional[str] = Field(title='description ')


class ConfigurationMapping(BaseModel):
    device: str
    mapping_fields: List[MappingField]


class StreamMappingField(MappingField):
    external: Optional[bool] = Field(default=False, description="Indicates whether field will be represented in the event directly or from an external file")


class ThumbnailInfo(BaseModel):
    number: int = Field(default=1, title="number of thumbnails", description="Specifies the number of thumbnails to produce")
    field: str = Field(title="thumbnail fields", description="Specifies the field to produce thumbails from")


class StreamMapping(BaseModel):
    mapping_fields: List[StreamMappingField]
    time_stamp: str = Field(title='time_stamp field', description='field to use to get time stamp values')
    conf_mappings: Optional[List[ConfigurationMapping]] = Field(title="event descriptor confguration")
    thumbnail_info: Optional[ThumbnailInfo] = Field(description="information about thumnails to produce from stream")


class Mapping(BaseModel):
    schema_version: int = SCHEMA_VERSION
    name: str = Field(title='Mapping name', description='Name of this mapping')
    description: str = Field(title='Mapping description', description='Description of this mapping')
    resource_spec: str = Field(title='Resource spec', description='databroker.handler spec for the resource document' +
                                                                  'produced by the ingestor. e.g. HDF, TIFFStack, etc.')
    md_mappings: Optional[List[MappingField]]
    stream_mappings: Optional[Dict[str, StreamMapping]]
    projections: Optional[List[Dict]]


class Issue(BaseModel):
    stage: str
    msg: str
    exception: Union[Exception, None]

    class Config:
        arbitrary_types_allowed = True
