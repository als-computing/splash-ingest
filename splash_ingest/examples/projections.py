projections = [
            {
                "name": "app_display",
                "version": "42.0.0",
                "configuration": {},
                "projection": {
                    'data': {
                        'type': 'linked',
                        'location': 'event',
                        'stream': 'primary',
                        'field': ':exchange:data',
                    },
                    'darks': {
                        'type': 'linked',
                        'location': 'event',
                        'stream': 'darks',
                        'field': ':exchange:dark',
                    },
                    'sample_name': {
                        'type': 'configuration', 
                        "field": ":measurement:sample:name",
                        'location': 'start',
                    },
                    'instrument_name':{
                       'type': 'configuration', 
                        "field": ":measurement:instrument:name",
                        'location': 'start', 
                    }
                }
            }
        ]