#!/bin/bash
dockeruser="shubhamat"
appname="client"


if [ "$(uname -m | grep -o arm)" = "arm" ]
then
    arch="arm"
    image="armhf/golang:1.6"
else
    arch="x86"
    image="golang:alpine"
fi
dockerimgname=$dockeruser"/"$appname"_"$arch
echo "Using image: $image"
echo "Compiling $appname for $arch..."
sudo docker run --rm -it -v "$(pwd)":/gocode -e "GOPATH=/gocode" -w /gocode $image sh -c "CGO_ENABLED=0 go build -a --installsuffix cgo --ldflags=\"-s\" -o $appname"
echo "Building $dockerimgname docker image..."
sudo docker build  -f Dockerfile.scratch -t $dockerimgname .

echo "Built image:"
sudo docker images $dockerimgname
