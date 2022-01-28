spec = "reader"


def ingest(scicat_client, *args, **kwargs):
    print("reader1 ingest called")
    return args[0]
