from setuptools import find_packages, setup

setup(
    name="event_log_analyzer",
    version="1.0.0",
    description="An Event Log Analyzer to Classify Event Logs According to Constraint Patterns",
    long_description_content_type="text/markdown",
    url="https://github.com/bz99/event_log_analyzer",
    author="Bernhard Zosel",
    author_email="s8bezose@stud.uni-saarland.de",
    license="MIT",
    packages=find_packages(),  
    install_requires=[
        "duckdb", 
        "graphviz",
        "jsonschema",
        "matplotlib",
        "networkx",
        "numpy",
        "pandas",
        "pm4py",
        #"pygraphviz",  #see requirements.in for more details
        "pytest",
        "Sphinx",
        "sphinx-rtd-theme"
    ],
    package_data={
        "": ["config_format.schema.json"]
    },
)
