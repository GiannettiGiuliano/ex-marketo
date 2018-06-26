FROM quay.io/keboola/docker-custom-python:latest

COPY . /code/

RUN pip install -r /code/requirements.txt

WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
