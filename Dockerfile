FROM apache/airflow:2.9.0 AS base
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Dev/experimentation image (docker-compose.dev.yml): production deps + test and notebook tooling
FROM base AS dev
COPY requirements-dev.txt /requirements-dev.txt
RUN pip install --no-cache-dir -r /requirements-dev.txt
# docker-compose.dev.yml runs as the host uid with gid 0; give the group the owner's access to $HOME
RUN chmod -R g=u /home/airflow
