FROM python:3.11
LABEL authors="misho"

EXPOSE 8000

WORKDIR /code

COPY requirements.txt /code/requirements.txt

RUN \
  apt-get update && \
  apt-get install -y default-jdk && \
  rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code/app

CMD python -m uvicorn app.main:app --host 0.0.0.0 --port 8000