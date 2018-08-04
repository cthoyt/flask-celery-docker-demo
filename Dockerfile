FROM python:3.7-alpine
MAINTAINER Charles Tapley Hoyt "cthoyt@gmail.com"

RUN pip install pipenv

COPY . /app
WORKDIR /app

# Could also use requirements.txt or setup.py for this part
RUN pipenv install --system --deploy
