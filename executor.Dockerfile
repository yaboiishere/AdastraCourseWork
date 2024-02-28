FROM bitnami/spark:3.4.2
LABEL authors="misho"

COPY requirements.txt /venv/requirements.txt
USER root
ENV VIRTUAL_ENV=/venv/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m pip install --no-cache-dir --upgrade -r /venv/requirements.txt

RUN chown -R 1001:1001 /venv
USER spark




