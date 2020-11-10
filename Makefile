
TAG    			:= $$(git describe --tags)
REGISTRY		:= registry.spin.nersc.gov
PROJECT 		:= dmcreyno
REGISTRY_NAME	:= ${REGISTRY}/${PROJECT}/${IMG}

NAME_WEB_WEB_SVC  	:= splash_ingest_webservice
IMG_WEB_SVC    		:= ${NAME_WEB_SVC}:${TAG}
REGISTRY_WEB_SVC	:= ${REGISTRY}/${PROJECT}/${NAME_WEB_SVC}:${TAG}

NAME_POLLER		:= splash_ingest_poller
IMG_POLLER   	:= ${NAME_POLLER}:${TAG}
REGISTRY_POLLER	:= ${REGISTRY}/${PROJECT}/${NAME_POLLER}:${TAG}

.PHONY: build

hello:
	@echo "Hello" ${REGISTRY}

build_service:
	@docker build -t ${IMG_WEB_SVC} -f Dockerfile-webservice .
	@echo "tagging to: " ${IMG_WEB_SVC}    ${REGISTRY_WEB_SVC}
	@docker tag ${IMG_WEB_SVC} ${REGISTRY_WEB_SVC}
 
push_service:
	@echo "Pushing " ${REGISTRY_WEB_SVC}
	@docker push ${REGISTRY_WEB_SVC}


build_poller:
	@docker build -t ${IMG_POLLER} -f Dockerfile-poller .
	@echo "tagging to: " ${IMG_POLLER}    ${REGISTRY_POLLER}
	@docker tag ${IMG_POLLER} ${REGISTRY_POLLER}


push_poller:
	@echo "Pushing " ${REGISTRY_POLLER}
	@docker push ${REGISTRY_POLLER}

login:
	@docker log -u ${DOCKER_USER} -p ${DOCKER_PASS}
	