FROM nethacker/ubuntu-18-04-python-3:python-3.7.3

WORKDIR /opt/app/src

COPY requirements.txt .
RUN pip install -r requirements.txt 

#ARG FLASK_ENV
#ENV FLASK_ENV=$FLASK_ENV

#ARG RELOAD
#ENV RELOAD=$RELOAD

#ENV PYTHONPATH="$WD/vouched:$PYTHONPATH"

#RUN ["rm", "-rf", "$WD/vouched/__pycache__"]
#RUN ["rm", "-rf", "$WD/vouched/test/__pycache__"]

RUN echo $MYENV

ENTRYPOINT ["python", "producer.py", "mx", "005"]
