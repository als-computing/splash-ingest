projections = [
            {
                "name": "app_display",
                "version": "42.0.0",
                "configuration": {},
                "projection": {
                    '/entry/instrument/detector/data': {
                        'type': 'linked',
                        'location': 'event',
                        'stream': 'primary',
                        'field': 'entry/instrument/detector/data',
                    },
#                     'sample_name': {
#                         'type': 'configuration', 
#                         "field": "/measurement/sample/name",
#                     }
                }
            }
        ]