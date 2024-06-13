from typing import List

from FlagEmbedding import BGEM3FlagModel
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# 初始化模型
model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=True)


# 定义一个Pydantic模型来描述请求体
class EmbeddingRequest(BaseModel):
    texts: List[str]


# 定义响应体模型
class EmbeddingResponse(BaseModel):
    embeddings: list[list[float]]


@app.post("/embeddings/")
async def generate_embeddings(request: EmbeddingRequest):
    try:
        # 使用模型进行编码
        data = request.texts
        embeddings = model.encode(data)["dense_vecs"]
        # 将numpy数组转换为列表，以便序列化
        embeddings_list = embeddings.tolist()
        return EmbeddingResponse(embeddings=embeddings_list)
    except Exception as e:
        # 如果发生错误，返回一个HTTP异常
        raise HTTPException(status_code=500, detail=str(e))
