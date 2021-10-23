FROM python:3.9.7-bullseye

WORKDIR /app

ARG POETRY_VERSION=1.1.11
RUN python -m pip install --upgrade pip && pip install --upgrade "poetry==$POETRY_VERSION"

COPY poetry.lock pyproject.toml /app/

ENV POETRY_VIRTUALENVS_CREATE=false
RUN poetry install --no-interaction --no-ansi

COPY . .

CMD [ "python", "./main.py" ]