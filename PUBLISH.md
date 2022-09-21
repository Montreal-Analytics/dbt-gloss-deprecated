# Publishing

Tutorial of publishing to help me not to forgot to anything :D

## 0. Bump new version

- setup.cfg
- __init__.py

## 1. Run tests

Run pytests:

```
pytest -vvv --cov=dbt_gloss --cov-config=setup.cfg --cov-report=term-missing --cov-report=html
```

Run pre-commit:

```
pre-commit run --all-files
```

Try import:

```
pre-commit try-repo .
```

## 2. Docker

Bump version in Dockerfile - .github/.pre-commit-config-action.yaml

Build:

```
docker build . -t Montreal-Analytics/dbt-gloss
docker tag Montreal-Analytics/dbt-gloss:latest Montreal-Analytics/dbt-gloss:<version>
```

Test:

```
docker run Montreal-Analytics/dbt-gloss
```

Publish to Docker Hub

```
docker push Montreal-Analytics/dbt-gloss
docker push Montreal-Analytics/dbt-gloss:<version>
```

## 3. Github Action

Bump docker version in action.yml

## 4. Write CHANGELOG

## 5. Push to Github

## 6. Create new Github deployment
