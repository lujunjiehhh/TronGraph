import numpy as np
import pandas as pd
from sko.PSO import PSO
from sko.tools import set_run_mode

from TronGraph import TronGraph
from TronGraph import get_score, get_score2

if __name__ == "__main__":
    def demo_func(x):
        # 前26个参数为W
        W = x[:19]
        # 最后两个参数为gamma和coef0
        gamma = x[-1]
        # coef0 = x[-1]
        addresses = "TP5Ct1fCmhRConWimVpx4U9MwiMuExi9m2"
        graph = TronGraph(W=W, gamma=gamma)
        nodes_df, _ = graph.explore_illegal_routes(
            start_address=addresses,
            tainted_weight=10000,
            max_nodes=75,
            is_sell=True,
            is_filter=True,
        )
        score1, node_list = get_score(
            rank1_end=14,
            rank2_end=27,
            addresses_df=pd.read_csv("address.csv"),
            nodes_df=nodes_df
        )
        score2 = get_score2(
            rank1_end=14,
            rank2_end=27,
            addresses_df=pd.read_csv("address.csv"),
            nodes_df=nodes_df
        )
        print("X:", W.reshape(1, -1))
        print(node_list)
        print("score1:", score1)
        print("score2:", score2)
        return score1

        # W = x
        # # 最后两个参数为gamma和coef0
        #
        # addresses = "TP5Ct1fCmhRConWimVpx4U9MwiMuExi9m2"
        #
        # score = TronGraph(W=W, gamma=0.1, coef0=0.1).get_score(
        #     rank1_end=14,
        #     rank2_end=27,
        #     start_address=addresses,
        #     addresses_df=pd.read_csv("address.csv"),
        #     max_node=75,
        # )
        # return score


    lb = list(np.ones(19) * -0.0000001)
    # lb[0] = -50000
    lb.append(0.01)

    up = list(np.ones(19) * 0.0000001)
    # up[0] = 50000
    up.append(1)

    # set_run_mode(demo_func, mode="multiprocessing")
    pso = PSO(
        func=demo_func,
        n_dim=20,
        pop=20,
        max_iter=5,
        lb=lb,
        ub=up,
        w=0.8,
        c1=0.5,
        c2=0.5,
    )  # type: ignore
    pso.run()

    print("best_x is ", pso.gbest_x, "best_y is", pso.gbest_y)
    #  写入文件
    with open("best_x_y.txt", "w") as f:
        f.write(f"best_x is {pso.gbest_x} \nbest_y is + {pso.gbest_y}")
    import matplotlib.pyplot as plt


    plt.plot(pso.gbest_y_hist)
    plt.show()
"""
loop 1
best_x [-0.06376446,0.00642685,-0.03161641,0.02644675,0.0733511,0.0050923,-0.00576532,0.02093931,0.07396938,0.1,0.04689537,-0.07767882,-0.09758978,0.06777194,0.0645549,0.05873585,-0.04266283,0.04135092,1.53254512,0.1]
best_y 100.7872
"""
