FROM python:3.9-slim AS build-env

COPY . /app
WORKDIR /app

RUN pip3 install --upgrade pip
RUN pip install keyring
RUN pip install keyrings.google-artifactregistry-auth
RUN pip install -r ./requirements.txt

FROM gcr.io/distroless/python3
COPY --from=build-env /app /app
COPY --from=build-env /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
WORKDIR /app

ENV PYTHONPATH=/usr/local/lib/python3.9/site-packages

CMD ["main.py", "/etc"]