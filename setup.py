#!/usr/bin/env python
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="llamakv",
    version="0.1.0",
    author="LlamaSearch.ai",
    author_email="info@llamasearch.ai",
    description="Flexible key-value storage system for LlamaSearch.ai applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/llamasearch/llamakv",
    project_urls={
        "Bug Tracker": "https://github.com/llamasearch/llamakv/issues",
        "Documentation": "https://docs.llamasearch.ai/llamakv",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "msgpack>=1.0.2",
        "redis>=4.0.0",
        "pylru>=1.2.0",
        "filelock>=3.4.0",
        "tenacity>=8.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.12.0",
            "black>=21.5b2",
            "flake8>=3.9.2",
            "mypy>=0.812",
            "isort>=5.9.1",
            "tox>=3.24.0",
        ],
        "distributed": [
            "redis-cluster>=2.1.0",
            "hiredis>=2.0.0",
        ],
        "docs": [
            "sphinx>=4.0.2",
            "sphinx-rtd-theme>=0.5.2",
            "sphinx-autodoc-typehints>=1.12.0",
        ],
    },
    include_package_data=True,
    zip_safe=False,
) 