import matplotlib.pyplot as plt
from tkinter import filedialog


def show_single_bench(path: str, d_filter: list = None):
    with open(path, "r") as f:
        head = f.readline().strip().split(",")
        data = [[] for _ in head]
        for line in f:
            for i, v in enumerate(line.strip().split(",")):
                data[i].append(float(v))
    x_axis_name = [h for h in head if h[0] == "_"][0]
    x_axis = data[head.index(x_axis_name)]
    head.remove(x_axis_name)
    data.remove(x_axis)
    if d_filter:
        data = [d for d, h in zip(data, head) if h in d_filter]
        head = [h for h in head if h in d_filter]
    for d, h in zip(data, head):
        plt.plot(x_axis, d, label=h)
    plt.xlabel(x_axis_name)
    plt.legend()
    plt.show()


if __name__ == "__main__":
    path = filedialog.askopenfilename()
    show_single_bench(path, ["CPU", "TIME"])
