VERSION=$(git describe --tags)
docker run -e SPLASH_LOG_LEVEL="DEBUG" --network host splash_ingest_poller:$VERSION