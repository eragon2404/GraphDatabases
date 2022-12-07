import matplotlib.pyplot as plt
from tkinter import Tk, IntVar, Checkbutton, Button, W, filedialog


def show_single_bench(path: str, to_show: list):
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
    data = [d for d, h in zip(data, head) if h in to_show]
    for d, h in zip(data, to_show):
        plt.plot(x_axis, d, label=h)
    plt.xlabel(x_axis_name[1:])
    plt.legend()
    plt.show()


def show_multiple_bench(paths: tuple[str], to_show: list, use_avg: bool):
    db_names = []
    paths_ = []
    for db in ["Orient", "Arango", "NEO4j"]:
        for path in paths:
            if db in path:
                db_names.append(db)
                paths_.append(path)
                break
    paths = paths_
    for val in to_show:
        for i, path in enumerate(paths):
            with open(path, "r") as f:
                head = f.readline().strip().split(",")
                index_d = head.index(val)
                index_x = head.index([h for h in head if h[0] == "_"][0])
                data = []
                x_axis = []
                for line in f:
                    data.append(float(line.strip().split(",")[index_d]))
                    x_axis.append(float(line.strip().split(",")[index_x]))
            if use_avg:
                plt.bar(db_names[i], sum(data) / len(data))
            else:
                plt.plot(x_axis, data, label=db_names[i])
        if not use_avg:
            plt.xlabel([h for h in head if h[0] == "_"][0][1:])
            plt.legend()
        plt.ylabel(val)
        plt.show()


def select_window(values: list):
    root = Tk()
    root.title("Select values")
    root.geometry("200x200")
    root.attributes("-topmost", True)
    selected = []
    use_avg = IntVar()
    for i, v in enumerate(values):
        var = IntVar()
        Checkbutton(root, text=v, variable=var).grid(row=i, sticky=W)
        selected.append(var)
    Checkbutton(root, text="Use average", variable=use_avg).grid(row=len(values), sticky=W)
    Button(root, text="OK", command=root.destroy).grid(row=len(values) + 1, sticky=W)
    root.update()
    root.mainloop()
    return [value for value, var in zip(values, selected) if var.get() == 1], use_avg.get() == 1


if __name__ == "__main__":
    paths = filedialog.askopenfilenames()
    print(paths)
    to_plot = []
    x_name = None
    for path in paths:
        with open(path, "r") as f:
            for val in f.readline().strip().split(","):
                if val[0] == "_":
                    if not x_name:
                        x_name = val
                    elif val != x_name:
                        raise ValueError("X axis is not the same")
                else:
                    if val not in to_plot:
                        to_plot.append(val)
    print(to_plot)
    to_plot, use_avg = select_window(to_plot)
    print(to_plot)
    if len(paths) == 1:
        show_single_bench(paths[0], to_plot)
    elif len(paths) > 1:
        show_multiple_bench(paths, to_plot, use_avg)
    else:
        pass
