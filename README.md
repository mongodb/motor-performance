# Motor performance benchmarks

Implement the [standard MongoDB Driver benchmark suite](https://jira.mongodb.org/browse/DRIVERS-301) for [Motor](https://motor.readthedocs.io).

Requires Python 3.5, Tornado, and Motor.

Clone the [Motor repository](https://github.com/mongodb/motor) and:

```
PYTHONPATH=/path/to/motor python3.5 perf_test.py
```

Simply installing Motor with pip isn't enough: the script uses testing code from Motor's repository.
