# Event Log Analyzer
The Event Log Analyzer is a python library that is able to import all different kinds of event logs and classify constraint patterns on these logs. For more information about constraint patterns, see https://constraintpatterns.com.

## Installation for users
The Event Log Analyzer has been developed and tested with Python 3.8.8.

We recommend to use a virtual environment, for example virtualenv (https://virtualenv.pypa.io). The following commands create such a virtual environment in a new project folder.
```shell script
mkdir ~/project_folder
cd ~/project_folder
virtualenv event_log_analyzer_env
source event_log_analyzer_env/bin/activate
```

The actual tool can easily be installed with pip (https://pip.pypa.io/en/stable/):
```shell script
pip3 install git+https://github.com/bernhardzosel/event-log-analyzer.git
```

## Installation for developers
For developers, the tool can be installed by cloning it from GitHub. For installing the dependencies we use a virtual environment, for example virtualenv (https://virtualenv.pypa.io).
```shell script
git clone https://github.com/bernhardzosel/event-log-analyzer.git ~/project_folder
cd ~/project_folder
virtualenv event_log_analyzer_env
source event_log_analyzer_env/bin/activate
pip3 install -r requirements.txt
```


## Usage
Here we show a simple example how to use our Event Log Analyzer package. Important for the import is the config file, where all information about the event log is specified. For trying out our tool, you can use example data from the `test/data` folder (see also in [Example Data](#example-data)).
In the example we simply import a log, print it out again and then check all patterns on the log.

```python
from event_log_analyzer import importer as event_log_importer
from event_log_analyzer.pattern_library import pattern_structure

log = event_log_importer.import_event_log("<path>/<config_file_name>.json")
log.print_event_log()

ps = pattern_structure.PatternStructure()
ps.check_all_patterns(log)
```


## Example Data
In the `test/data` folders we provide example datasets, i.e. real event logs as well as generated logs in interval and atomic format. The corresponding config file to a dataset `<name>.csv` can be found in the `test/data` folder under the name `<name>.json`.

## Documentation
The documentation is located in the docs folder. Open 
`docs/build/html/index.html`
to view the documentation in your browser. A PDF version of the documentation can be found in `docs/build/latex/event_log_analyzer.pdf`.
