import re
from setuptools import setup

with open("src/dataloader/__init__.py", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="DataLoader",
    version=version,
    install_requires=[
        # "psycopg2-binary>=2.8.5",
        # "sqlalchemy >= 1.3.16",
        # "pytz == 2020.1",
        # "factory-boy == 2.12.0"
    ],
    extras_require={
        "dotenv": ["python-dotenv"],
        "dev": [
            "pytest",
            "coverage"
        ],
    },
)