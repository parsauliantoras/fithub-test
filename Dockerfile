FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

CMD ["python", "main.py"]