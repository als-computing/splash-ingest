mapping_dict = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "test_spec",
        "metadata_mappings": {
            "/measurement/sample/name": "dataset",
            "/measurement/instrument/name": "end_station",
            "/measurement/instrument/source/beamline": "beamline"
        },
        "stream_mappings": {
            "primary":{
                "time_stamp": "/process/acquisition/image_date",
                "mapping_fields": {
                    "/exchange/data": "data",
                 #   "/process/acquisition/sample_position_x": "tile_xmovedist"
                }

            }
        }
}