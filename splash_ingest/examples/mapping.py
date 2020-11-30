mapping_dict = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "/measurement/sample/name"},
            {"field": "/measurement/instrument/name"},
            {"field": "/measurement/sample/experimenter/name"},
            {"field": "/measurement/sample/experiment/proposal"},
            {"field": "/measurement/sample/experiment/title"},
            {"field": "/process/acquisition/start_date"}
        ],
        "stream_mappings": {
            "primary":{
                "time_stamp": "/defaults/NDArrayTimeStamp",
                "mapping_fields": [
                    {"field": "/exchange/data", "external": True},
                ]

            }
        }
}