FROM python:3.8


COPY ./requirements.txt /tmp/
COPY ./requirements-webservices.txt /tmp/
RUN pip install -U pip &&        pip install -r /tmp/requirements.txt &&        pip install -r /tmp/requirements-webservices.txt
COPY ./ /app
