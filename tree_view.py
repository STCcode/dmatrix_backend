import os

def print_tree(start_path, prefix=""):
    items = os.listdir(start_path)
    for index, item in enumerate(sorted(items)):
        path = os.path.join(start_path, item)
        connector = "└── " if index == len(items) - 1 else "├── "
        print(prefix + connector + item)
        if os.path.isdir(path) and item not in ["venv", "__pycache__", ".git", "node_modules"]:
            new_prefix = prefix + ("    " if index == len(items) - 1 else "│   ")
            print_tree(path, new_prefix)

print_tree(".")
