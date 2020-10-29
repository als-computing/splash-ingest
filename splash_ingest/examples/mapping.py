mapping_dict = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "/measurement/sample/name"},
            {"field": "/measurement/instrument/name"}
        ],
        "stream_mappings": {
            "primary":{
                "time_stamp": "/process/acquisition/image_date",
                "mapping_fields": [
                    {"field": "/exchange/data", "external": True},
                ]

            },
             "darks":{
                "time_stamp": "/process/acquisition/image_date",
                "mapping_fields": [
                    {"field": "/exchange/dark", "external": True},
                ]

            },

        }
}