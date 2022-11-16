import matplotlib.pyplot as plt
from tkinter import filedialog


def show_single_bench(path: str, ):
    time = []
    cpu_usage = []
    memory_usage = []
    with open(path, "r") as file:
        for line in file:
            if line[0] == "#" or line[0] == "t" or line == "":
                continue
            t, cpu, mem = line.strip().split(",")
            time.append(float(t))
            cpu_usage.append(float(cpu))
            memory_usage.append(float(mem))
    plt.plot(time, cpu_usage, label="CPU usage")
    # secon y axis for memory
    ax2 = plt.twinx()
    ax2.plot(time, memory_usage, label="Memory usage", color="red")
    #plt.plot(time, memory_usage, label="Memory usage")
    plt.xlabel("Time")
    plt.ylabel("CPU usage")
    ax2.set_ylabel("Memory usage")
    #plt.ylabel("Usage")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    path = filedialog.askopenfilename()
    show_single_bench(path)
