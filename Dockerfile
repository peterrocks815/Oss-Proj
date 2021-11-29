FROM python:3.6 as builder

RUN mkdir -p /home/app
RUN mkdir -p /home/app/templates
RUN mkdir -p /home/app/input

ADD server.py /home/app
ADD main.py /home/app

COPY /Oss-Proj/templates /home/app/templates
COPY requirements.txt /home/app

FROM python:3.6
WORKDIR /app
COPY --from=builder /home/app/ /app/

RUN pip install -r /app/requirements.txt
RUN ["python3", "/app/server.py"]