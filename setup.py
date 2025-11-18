"""Setup configuration for mongolib"""

from setuptools import setup, find_packages

with open("README.md", "w") as f:
    f.write("""# MongoLib

Modern, modular Python MongoDB library with comprehensive features.

## Features

- Modern MongoDB connection management
- Multiple document factories (dict, Document, named tuple)
- Custom type adapters and converters
- Connection pooling
- Query builder
- Schema management
- Migration utilities
- Transaction management
- MongoDB-specific features (Change Streams, Aggregation Pipeline Builder)
- Interactive CLI
- Type hints and modern Python features

## Installation

```bash
pip install -e .
```

## Dependencies

This library requires `pymongo`:

```bash
pip install pymongo
```

## Quick Start

```python
from mongolib import connect

with connect("mongodb://localhost:27017/mydb") as conn:
    collection = conn.collection("users")
    collection.insert_one({"name": "Alice", "age": 30})
    
    cursor = conn.execute("users", "find", {"age": {"$gt": 25}})
    for doc in cursor:
        print(doc)
```

## Connection Pooling

```python
from mongolib.advanced import create_pool

pool = create_pool(dsn="mongodb://...", minconn=1, maxconn=10)

with pool.connection() as conn:
    cursor = conn.execute("users", "find", {})
    results = cursor.fetchall()
```

## Query Builder

```python
from mongolib.advanced import query

q = query(conn, "users")
results = q.filter(age={"$gt": 18}).sort("name").limit(10).fetchall()
```

## Aggregation Pipeline

```python
from mongolib.specific import create_pipeline

pipeline = create_pipeline(conn, "users")
results = pipeline.match({"age": {"$gt": 18}}).group({
    "_id": "$city",
    "count": {"$sum": 1}
}).fetchall()
```

## CLI Usage

```bash
python -m mongolib.utilities.cli mongodb://user:pass@localhost:27017/dbname
```
""")

setup(
    name="mongolib",
    version="1.0.0",
    description="Modern, modular Python MongoDB library",
    long_description=open("README.md", "r", encoding="utf-8").read() if True else "",
    long_description_content_type="text/markdown",
    author="MongoLib Contributors",
    packages=find_packages(where="."),
    package_dir={"mongolib": "mongolib"},
    python_requires=">=3.10",
    install_requires=[
        "pymongo>=4.0.0",
    ],
    extras_require={
        "dev": ["pytest>=7.0.0"],
    },
    entry_points={
        "console_scripts": [
            "mongolib=mongolib.utilities.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)

