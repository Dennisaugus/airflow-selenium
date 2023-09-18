FROM apache/airflow:2.3.2-python3.7

ENV PYTHONPATH="/opt/airflow/code/:/opt/airflow/include/:${PYTHONPATH}"
USER root

# Instalar as dependências do sistema operacional
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libgconf-2-4 \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxtst6 \
    libgbm-dev \
    xvfb

# Instalar o Chrome e o ChromeDriver
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
    apt-get update && apt-get install -y google-chrome-stable

# Configurar o ChromeDriver
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/$(wget -q -O - https://chromedriver.storage.googleapis.com/LATEST_RELEASE)/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/

#RUN pip install 'apache-airflow[amazon]'

COPY ./requirements.txt /requirements.txt
#COPY ./.ssh/.ssh/ /home/airflow/.ssh/
#RUN chown -R airflow /home/airflow/.ssh/

#RUN mkdir -p /code/app
##RUN mkdir -p /data
#COPY ./app /code/app/
#RUN chown -R airflow /code/app/
#RUN chown -R airflow /data/

USER airflow

RUN pip install -r  /requirements.txt