
file_name = input("Enter file name: ")

nodes = []
# example line: 30	 1412
with open(file_name, "r") as f:
    for line in f:
        if line[0] != "#":
            node1, node2 = line.strip().split("\t")
            if node1 not in nodes:
                nodes.append(node1)
            if node2 not in nodes:
                nodes.append(node2)
with open(file_name + "_nodes", "w") as f:
    for node in nodes:
        f.write(node + "\n")
