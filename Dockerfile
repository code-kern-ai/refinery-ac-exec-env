FROM python:3.10-slim

RUN apt update && apt install -y curl

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["/run.sh"] 
