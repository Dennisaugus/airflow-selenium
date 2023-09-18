FROM continuumio/miniconda3

# Creating project directory and defining it as workdir
RUN mkdir -p /src
WORKDIR /src

# Installing dependencies
COPY requirements.yml .
RUN conda env create -f requirements.yml
# RUN conda init bash

# Copying code files
COPY model ./model
COPY run_model.py .

# Running the API
CMD ["conda", "run", "--no-capture-output", "-n", "model", "python", "run_model.py"]
