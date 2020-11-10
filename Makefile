
TAG    			:= $$(git describe --tags)
REGISTRY		:= registry.spin.nersc.gov
PROJECT 		:= dmcreyno
REGISTRY_NAME	:= ${REGISTRY}/${PROJECT}/${IMG}

NAME_SVC  		:= splash_ingest_webservice
IMG_SVC    		:= ${NAME_SVC}:${TAG}
REGISTRY_SVC	:= ${REGISTRY}/${PROJECT}/${NAME_SVC}

NAME_POLLER		:= splash_ingest_poller
IMG_POLLER   	:= ${NAME_POLLER}:${TAG}
REGISTRY_POLLER	:= ${REGISTRY}/${PROJECT}/${NAME_POLLER}

.PHONY: build

hello:
	@echo "Hello" ${REGISTRY}

build_service:
	@docker build -t ${IMG_SVC} -f Dockerfile-webservice .
	@echo "tagging to: " ${IMG_SVC}    ${REGISTRY_SVC}
	@docker tag ${IMG_SVC} ${REGISTRY_SVC}
 
push_service:
	@echo "Pushing " ${REGISTRY_NAME}
	@docker push ${REGISTRY_NAME}


build_poller:
	@docker build -t ${IMG_POLLER} -f Dockerfile-poller .
	@echo "tagging to: " ${IMG_POLLER}    ${REGISTRY_POLLER}
	@docker tag ${IMG_POLLER} ${REGISTRY_POLLER}

login:
	@docker log -u ${DOCKER_USER} -p ${DOCKER_PASS}
	