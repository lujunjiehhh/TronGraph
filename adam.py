import pandas as pd
import torch
import torch.optim as optim
from TronGraph import TronGraph


def demo_func(x):
    # 前26个参数为W
    w = x[:18]
    addresses = "TP5Ct1fCmhRConWimVpx4U9MwiMuExi9m2"

    # # 最后两个参数为gamma和coef0
    # gamma = x[-2]
    # coef0 = x[-1]

    gamma = 0
    coef0 = 0

    score = TronGraph(W=w, gamma=gamma, coef0=coef0).get_score_for_adam(
        rank1_end=14,
        rank2_end=27,
        start_address=addresses,
        addresses_df=pd.read_csv("address.csv"),
        max_node=75,
    )
    return score


params = torch.randn(18, requires_grad=True)
optimizer = optim.Adam([params], lr=0.001)
# 执行优化循环
num_epochs = 10
for epoch in range(num_epochs):
    # 清零梯度
    optimizer.zero_grad()

    # 前向传播
    # 计算目标函数的相反数
    outputs = -demo_func(params)

    # 计算损失
    loss = outputs

    # 反向传播
    print(loss)
    loss.backward()

    # 执行优化步骤
    optimizer.step()

    # 打印损失
    print(f"Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item()}")
