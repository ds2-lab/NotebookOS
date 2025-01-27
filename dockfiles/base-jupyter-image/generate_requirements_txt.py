import tomli  # or "import tomllib" for Python 3.11+
import sys

def generate_requirements(pyproject_path="pyproject.toml", output_path="./distributed_notebooks_requirements.txt"):
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomli.load(f)

    # Extract dependencies
    dependencies = pyproject_data.get("project", {}).get("dependencies", [])

    with open(output_path, "w") as f:
        for dep in dependencies:
            f.write(dep + "\n")

    print(f"Generated {output_path} from {pyproject_path}")

if __name__ == "__main__":
    generate_requirements(pyproject_path = "../../pyproject.toml")
