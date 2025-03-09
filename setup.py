from setuptools import setup, find_packages

setup(
    name="perm-scraper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pymongo",
        "python-dotenv",
        "schedule",
    ],
    python_requires=">=3.8",
)