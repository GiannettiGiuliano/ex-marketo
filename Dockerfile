FROM quay.io/keboola/docker-custom-python:latest

COPY . /code/

RUN pip install -r /code/requirements.txt

WORKDIR /code/
CMD ["python", "-u", "/code/main.py"]
