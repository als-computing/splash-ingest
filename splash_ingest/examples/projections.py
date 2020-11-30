projections = [
            {
            "name": "app_display mona",
            "configuration": {},
            "projection": {
                "image_data": {
                    "type": "linked",
                    "location": "event",
                    "stream": "primary",
                    "field": ":exchange:data"
                },
                "sample_name": {
                    "type": "configuration", 
                    "field": ":measurement:sample:name",
                    "location": "start"
                },
                "instrument_name": {
                    "type": "configuration", 
                    "field": ":measurement:instrument:name",
                    "location": "start"
                },
                "experimenter":{
                    "type": "configuration", 
                    "field": ":measurement:sample:experimenter:name",
                    "location": "start"
                },
                "proposal":{
                    "type": "configuration", 
                    "field": ":measurement:sample:experiment:proposal",
                    "location": "start"
                },
                "title":{
                    "type": "configuration", 
                    "field": ":measurement:sample:experiment:title",
                    "location": "start"
                },
                "start_date":{
                    "type": "configuration", 
                    "field": ":process:acquisition:start_date",
                    "location": "start"
                }
            }
        }
    ]