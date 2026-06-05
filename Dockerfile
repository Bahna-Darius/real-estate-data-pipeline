FROM ubuntu:latest
LABEL authors="darius"

ENTRYPOINT ["top", "-b"]