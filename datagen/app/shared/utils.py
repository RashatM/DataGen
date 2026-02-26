import random
from typing import Any, List

import matplotlib.pyplot as plt
import networkx as nx


def visualize_graph(graph):
    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(graph)  # Оптимальная раскладка узлов
    nx.draw(graph, pos, with_labels=True, node_color="lightblue", node_size=2000, font_size=10, font_weight="bold")
    plt.title("Граф зависимостей таблиц")
    plt.show()


def random_choices_from_constants(constants: List[Any], length: int) -> List[Any]:
    return random.choices(constants, k=length) if len(constants) > 1 else [constants[0] for _ in range(length)]


def shuffle_values_with_nulls(target_count: int, values: List[Any]) -> List[Any]:
    values = [None] * target_count + values
    random.shuffle(values)
    return values


