import asyncio
import asyncio_redis
import awswrangler as wr
import boto3
import logging

from utils import get_data
from cfg import AppCfg

# for s3 and s3a protocol compatibility
wr.config.s3_endpoint_url  = 'http://minio:9000'

logging.getLogger().setLevel(logging.INFO)


minio_session = boto3.session.Session(
    aws_access_key_id=AppCfg.MINIO_ACCESS_KEY,
    aws_secret_access_key=AppCfg.MINIO_SECRET_KEY,
)


async def load_data():
    # redis connection
    connection = await asyncio_redis.Connection.create(host=AppCfg.REDIS_HOST, port=AppCfg.REDIS_PORT)

    data_iter = wr.s3.read_parquet(
        AppCfg.IN_PATH, boto3_session=minio_session, chunked=10
    )

    for idx, stream in enumerate(data_iter):
        logging.info(f"Loading {idx} streams")
        for d_idx, row in stream.iterrows():
            user, features = get_data(row)
            await connection.set(str(user), features)

    # close the connection
    connection.close()


if __name__ == '__main__':
    logging.info(f"Uploading user features to online store...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(load_data())