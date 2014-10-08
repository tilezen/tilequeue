# Contributing

## Issues

If you notice a problem, or would like to request a feature, create an
issue within the [github issue
tracker](https://github.com/mapzen/tilequeue/issues).

## Pull Requests

* Fork the repository
* Work off a branch
* Add a test if possible
* All tests should pass via `python setup.py test`
* Ensure that the code is
  [pep8](http://legacy.python.org/dev/peps/pep-0008/) compliant (use
  [flake8](https://pypi.python.org/pypi/flake8))
  - `find . -name '*.py' | xargs flake8`
* Use atomic commits, and avoid adding multiple features or fixes in a
  single pull request. Open multiple pull requests, one for each
  semantic change.
* Use good [commit messages](http://git-scm.com/book/ch5-2.html)
