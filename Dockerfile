FROM totvslabs/pycarol:2.38.0

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

CMD ["python", "run.py"]
