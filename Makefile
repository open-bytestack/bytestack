.PHONY: proto proto-image

proto-image:
	./hack/build-proto-image.sh

proto:
	./hack/generate-proto.sh
