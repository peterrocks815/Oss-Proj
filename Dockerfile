FROM python:3.6 as builder

RUN mkdir -p /home/app
RUN mkdir -p /home/app/templates
RUN mkdir -p /home/app/input

ADD /Oss-Proj/server.py /home/app
ADD /Oss-Proj/main.py /home/app

COPY /Oss-Proj/templates /home/app/templates
COPY /Oss-Proj/requirements.txt /home/app

FROM python:3.6
WORKDIR /app
COPY --from=builder /home/app/ /app/
ENV path_var=/home/christoph/testing/Oss-Proj connector_var=filesystem

RUN pip install -r /app/requirements.txt
RUN ["python3", "/app/server.py"]
