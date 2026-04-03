from setuptools import setup, find_packages

setup(
    name="netfleet-shared",
    version="2.0.0",
    description=(
        "Shared package for NetFleet — "
        "Distributed Network Device Management Platform"
    ),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Jatin Kumar Rout",
    python_requires=">=3.10",
    packages=find_packages(
        include=[
            "shared",
            "shared.*",
            "plugins",
            "plugins.*"
        ]
    ),
    install_requires=[
        "pydantic==2.5.0",
        "python-dotenv==1.0.0",
        "pyyaml==6.0.1",
        "redis==5.0.1",
        "pymongo==4.6.0",
        "confluent-kafka==2.3.0",
        "motor==3.3.2",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.4",
            "pytest-asyncio==0.23.3",
            "pytest-mock==3.12.0",
            "black==23.12.0",
            "isort==5.13.0",
        ]
    }
)
