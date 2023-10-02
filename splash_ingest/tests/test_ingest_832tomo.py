from splash_ingest.ingestors import ingest_tomo832



def test_clean_email():
    assert ingest_tomo832.clean_email(" 'slartibartfast@magrathea.gov' ") == "slartibartfast@magrathea.gov"
    assert ingest_tomo832.clean_email("slartibartfast@magrathea.gov") == "slartibartfast@magrathea.gov"
    assert ingest_tomo832.clean_email(None) == None