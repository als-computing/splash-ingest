mapping_dict = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "metadata_mappings": {
            "/measurement/sample/name": "",
            "/measurement/instrument/name":""
        },
        "stream_mappings": {
            "primary":{
                "time_stamp": "/process/acquisition/image_date",
                "mapping_fields": [
                    {"name":"/exchange/data"}
                ]

            },
             "darks":{
                "time_stamp": "/process/acquisition/image_date",
                "mapping_fields": [
                    {"name":"/exchange/dark"}
                ]

            },

        }
}