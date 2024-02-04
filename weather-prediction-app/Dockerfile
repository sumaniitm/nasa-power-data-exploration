FROM python:3.9.7-slim

USER 0
RUN mkdir -p /mnt/shared/
RUN mkdir -p /mnt/shared/s3_downloads/

WORKDIR /script

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY script/ /script

ENV PYTHONPATH=/mnt/shared/:$PYTHONPATH

ENTRYPOINT ["python3", "/script/app.py", "-x"]