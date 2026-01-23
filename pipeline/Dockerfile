# base Docker image that we will build on
# Start with slim Python 3.13 image
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml  .python-version uv.lock ./
RUN uv sync --locked

# set up our image by installing prerequisites; pandas in this case
#RUN pip install pandas pyarrow

# Set working directory
#WORKDIR /code

# Copy application code
# copy the script to the container. 1st name is source file, 2nd is destination

# COPY pipeline.py .
COPY ingest_data.py .

# Set entry point
#ENTRYPOINT ["uv","run","python", "pipeline.py"]

# Os parâmetros ("uv","run",) foram incluidos do PATH
#ENTRYPOINT ["python", "pipeline.py"]
ENTRYPOINT ["python", "ingest_data.py"]