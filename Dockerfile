FROM python:3.11-bullseye

ENV PIPENV_VENV_IN_PROJECT=1
ENV PIPENV_IGNORE_VIRTUALENVS=1

WORKDIR /budget_app

COPY Pipfile Pipfile.lock ./

RUN pip install --no-cache-dir pipenv && pipenv install --system --deploy

COPY . .

CMD ["pipenv", "run", "gunicorn", "--bind=0.0.0.0:8080", "budget_app.wsgi:app"]