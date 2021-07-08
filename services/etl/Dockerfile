FROM python:3
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt
CMD ["load_features_kvs.py"]
ENTRYPOINT ["python3"]
