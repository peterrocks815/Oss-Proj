FROM python:3.6

RUN mkdir -p /home/app

ADD main.py /home/app

COPY requirements.txt /home/app
COPY input/config /home/app
COPY input/data.csv /home/app
COPY input/schema.txt /home/app

RUN pip install -r /home/app/requirements.txt

ENTRYPOINT ["python3", "/home/app/main.py", "/home/app/data.csv", "/home/app/schema.txt", "/home/app/config"]