FROM apache/airflow:2.2.3-python3.8

USER root

RUN apt-get update 
#RUN apt-get install -y --no-install-recommends firefox

RUN rm -rf /var/lib/apt/lists/*

RUN pip install selenium webdriver_manager

USER airflow


USER root
RUN mkdir /usr/local/bin/geckodriver
# Adicione as seguintes linhas para definir as permissões no diretório do WebDriver Manager
RUN chown -R airflow: /usr/local/bin/geckodriver
RUN chmod +x /usr/local/bin/geckodriver

USER airflow