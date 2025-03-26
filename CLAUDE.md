# CLAUDE.md - Guidelines for Kafka Clients Repository

## Build and Test Commands
- Install dependencies: `pip install -r requirements.txt` or `mvn install`
- Run tests: `pytest tests/` or `mvn test`
- Run single test: `pytest tests/test_file.py::test_name`
- Lint code: `flake8 .` or `mvn checkstyle:check`
- Type check: `mypy .` or `javac -Xlint:all`

## Code Style Guidelines
- **Formatting**: Follow PEP8 for Python, Google Java Style Guide for Java
- **Imports**: Group imports (stdlib, third-party, local) with a blank line between groups
- **Naming**: snake_case for Python (variables, functions), camelCase for Java/Scala
- **Types**: Use type hints in Python, proper generics in Java/Scala
- **Error Handling**: Use specific exceptions, proper logging with levels
- **Documentation**: Docstrings for all public functions, classes and modules
- **Testing**: Write unit tests for all new features, aim for >80% coverage
- **Logging**: Use structured logging with appropriate log levels
- **Commits**: Write descriptive commit messages following conventional commits

Keep Kafka client implementations consistent across languages where possible.