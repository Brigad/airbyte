FROM ghcr.io/dbt-labs/dbt-core:1.3.1
COPY --from=airbyte/base-airbyte-protocol-python:0.1.1 /airbyte /airbyte

# Install SSH Tunneling dependencies
RUN apt-get update && apt-get install -y jq sshpass bash

WORKDIR /airbyte
COPY entrypoint.sh .
COPY build/sshtunneling.sh .

WORKDIR /airbyte/normalization_code
COPY normalization ./normalization
COPY setup.py .
COPY dbt-project-template/ ./dbt-template/
COPY dbt-project-template-databricks/* ./dbt-template/

# Install python dependencies
WORKDIR /airbyte/base_python_structs
# workaround for https://github.com/yaml/pyyaml/issues/601
# this should be fixed in the airbyte/base-airbyte-protocol-python image
RUN pip install "Cython<3.0" "pyyaml==5.4" --no-build-isolation

RUN pip install .

WORKDIR /airbyte/normalization_code
RUN pip install .
RUN pip install "dbt-databricks>=1.3.1"

WORKDIR /airbyte/normalization_code/dbt-template/
# Download external dbt dependencies
RUN dbt deps --profiles-dir /airbyte/normalization_code/dbt-template

WORKDIR /airbyte
ENV AIRBYTE_ENTRYPOINT "/airbyte/entrypoint.sh"
ENTRYPOINT ["/airbyte/entrypoint.sh"]

LABEL io.airbyte.name=airbyte/normalization-databricks
