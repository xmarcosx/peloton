FROM python:3.8.3

WORKDIR /workspaces/peloton
COPY ./ .

RUN pip install --no-cache-dir pipenv
RUN pipenv install --system --deploy

CMD ["python", "peloton/main.py"]
