FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7


WORKDIR /

COPY . /app

RUN pip install -r app/requirements.txt

EXPOSE 80

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
