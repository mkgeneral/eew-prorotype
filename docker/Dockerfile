FROM nethacker/ubuntu-18-04-python-3:python-3.7.3

WORKDIR /opt/app/src

COPY requirements.txt .
RUN pip install -r requirements.txt 


ENTRYPOINT ["python", "producer.py", "mx", "005"]
