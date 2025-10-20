from __future__ import annotations

import aioboto3
from typing import AsyncGenerator, Optional
from settings.config import settings


class S3Client:
    def __init__(self, region: Optional[str] = None):
        self.region = region or settings.AWS_REGION

    async def put_text(self, bucket: str, key: str, text: str, content_type: str = "application/json") -> str:
        session = aioboto3.Session()
        async with session.client("s3", region_name=self.region) as s3:
            await s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)
        return f"s3://{bucket}/{key}"

    async def put_stream(self, bucket: str, key: str, stream, content_type: str) -> str:
        session = aioboto3.Session()
        async with session.client("s3", region_name=self.region) as s3:
            await s3.upload_fileobj(stream, Bucket=bucket, Key=key, ExtraArgs={"ContentType": content_type})
        return f"s3://{bucket}/{key}"

    async def presigned_get(self, bucket: str, key: str, expires: int = 3600) -> str:
        session = aioboto3.Session()
        async with session.client("s3", region_name=self.region) as s3:
            url = await s3.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires,
            )
            return url


