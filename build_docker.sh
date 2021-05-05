#!/bin/bash

set -e 
set -o pipefail

make build_service
make push_service
make build_poller
make push_poller
